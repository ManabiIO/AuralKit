import Foundation
import Testing
@testable import AuralKit

@available(iOS 26.0, macOS 26.0, *)
@Suite("Speech installation and borrowed reservations")
struct UpstreamSpeechAssetReservationTests {
    @Test("overlapping managers retain the locale until the final owner stops")
    func overlappingManagers() async throws {
        let inventory = AssetInventoryProbe()
        let reservations = SpeechAssetReservations(inventory: inventory.client)
        let first = ModelManager(reservations: reservations, assetDownloader: { _ in })
        let second = ModelManager(reservations: reservations, assetDownloader: { _ in })
        let module = ReservationModule(locales: [Locale(identifier: "en-US")])
        try await first.ensureAssets(for: [module])
        try await second.ensureAssets(for: [module])
        await first.releaseLocales()
        #expect(await inventory.snapshot.releases.isEmpty)
        await second.releaseLocales()
        await second.releaseLocales()
        let state = await inventory.snapshot
        #expect(state.reserves == ["en-US"])
        #expect(state.releases == ["en-US"])
        #expect(state.locales.isEmpty)
    }

    @Test("borrowed external reservations survive cleanup", arguments: ["en-US", "en-AU"])
    func borrowedReservation(alias: String) async throws {
        let inventory = AssetInventoryProbe(initial: [Locale(identifier: "en-US")],
                                            variants: ["en-AU": Locale(identifier: "en-US")])
        let reservations = SpeechAssetReservations(inventory: inventory.client)
        let manager = ModelManager(reservations: reservations, assetDownloader: { _ in })
        try await manager.ensureAssets(for: [ReservationModule(locales: [Locale(identifier: alias)])])
        await manager.releaseLocales()
        #expect(await inventory.snapshot.releases.isEmpty)
        #expect(await inventory.snapshot.locales == ["en-US"])
    }

    @Test("variant holders protect the observed native reservation")
    func canonicalVariant() async throws {
        let inventory = AssetInventoryProbe(variants: ["en-AU": Locale(identifier: "en-US"),
                                                      "en-GB": Locale(identifier: "en-US")])
        let reservations = SpeechAssetReservations(inventory: inventory.client)
        let owner = try await reservations.acquire(Locale(identifier: "en-AU"))
        let borrower = try await reservations.acquire(Locale(identifier: "en-GB"))
        let canonical = try await reservations.acquire(Locale(identifier: "en-US"))
        await reservations.release(owner)
        await reservations.release(canonical)
        #expect(await inventory.snapshot.releases.isEmpty)
        await reservations.release(borrower)
        #expect(await inventory.snapshot.locales.isEmpty)
    }

    @Test("canceled or retired installation work cannot reserve a locale", arguments: [true, false])
    func canceledQueuedInstallation(cancel: Bool) async throws {
        let inventory = AssetInventoryProbe()
        let native = inventory.client
        let reservations = SpeechAssetReservations(inventory: native)
        let blockerGate = AssetGate()
        let blocker = Task {
            try await reservations.withInventoryAccess { await blockerGate.wait() }
        }
        await blockerGate.waitForEntry()
        let manager = ModelManager(reservations: reservations, installationRequest: { _ in
            _ = try await native.reserve(Locale(identifier: "en-US"))
            return nil
        })
        let module = ReservationModule(locales: [Locale(identifier: "en-US")])
        let preparation = Task { try await manager.ensureAssets(for: [module]) }
        let deadline = ContinuousClock.now + .seconds(1)
        while await reservations.queuedOperations < 2, ContinuousClock.now < deadline {
            try await Task.sleep(for: .milliseconds(1))
        }
        #expect(await reservations.queuedOperations == 2)
        var cleanup: Task<Void, Never>?
        if cancel {
            preparation.cancel()
        } else {
            cleanup = Task { await manager.releaseLocales() }
            let cleanupDeadline = ContinuousClock.now + .seconds(1)
            while await manager.cleanupTask == nil, ContinuousClock.now < cleanupDeadline {
                await Task.yield()
            }
            #expect(await manager.cleanupTask != nil)
        }
        await blockerGate.open()
        try await blocker.value
        if case .success = await preparation.result { Issue.record("Retired preparation succeeded") }
        await cleanup?.value
        await manager.releaseLocales()
        #expect(await inventory.snapshot.locales.isEmpty)
        let successor = ModelManager(reservations: reservations, assetDownloader: { _ in })
        try await successor.ensureAssets(for: [module])
        await successor.releaseLocales()
        #expect(await inventory.snapshot.locales.isEmpty)
    }

    @Test("implicit installation reservations cannot invalidate an explicit acquisition")
    func concurrentInstallationRequest() async throws {
        let inventory = AssetInventoryProbe()
        let native = inventory.client
        let gate = AssetGate()
        let reservations = SpeechAssetReservations(inventory: .init(
            locales: native.locales,
            reserve: { locale in
                let created = try await native.reserve(locale)
                await gate.wait()
                return created
            }, release: native.release
        ))
        let owner = Task { try await reservations.acquire(Locale(identifier: "en-US")) }
        await gate.waitForEntry()
        let installation = Task {
            try await reservations.withInventoryAccess {
                _ = try await native.reserve(Locale(identifier: "fr-FR"))
            }
        }
        await gate.open()
        let lease = try await owner.value
        try await installation.value
        await reservations.release(lease)
        #expect(await inventory.snapshot.releases == ["en-US"])
        #expect(await inventory.snapshot.locales == ["fr-FR"])
    }

    @Test("reacquisition waits for a pending native release")
    func reacquisitionDuringRelease() async throws {
        let inventory = AssetInventoryProbe()
        let reservations = SpeechAssetReservations(inventory: inventory.client)
        let lease = try await reservations.acquire(Locale(identifier: "en-US"))
        let gate = AssetGate()
        await inventory.holdRelease(at: gate)
        let release = Task { await reservations.release(lease) }
        await gate.waitForEntry()
        let next = Task { try await reservations.acquire(Locale(identifier: "en-US")) }
        await gate.open()
        await release.value
        let nextLease = try await next.value
        #expect(await inventory.snapshot.locales == ["en-US"])
        await reservations.release(nextLease)
        #expect(await inventory.snapshot.reserves == ["en-US", "en-US"])
        #expect(await inventory.snapshot.releases == ["en-US", "en-US"])
    }
}
