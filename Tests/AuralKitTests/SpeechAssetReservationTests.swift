import Foundation
import Speech
import Testing
@testable import AuralKit

@available(iOS 26.0, macOS 26.0, *)
private actor AssetGate {
    private(set) var entered = false
    private var isOpen = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        entered = true
        guard !isOpen else { return }
        await withCheckedContinuation { waiters.append($0) }
    }

    func open() {
        isOpen = true
        let pending = waiters
        waiters.removeAll()
        for waiter in pending { waiter.resume() }
    }
}

@available(iOS 26.0, macOS 26.0, *)
private actor AssetInventoryProbe {
    struct Snapshot: Sendable {
        let locales: Set<String>
        let reserves: [String]
        let releases: [String]
    }

    private var stored: [String: Locale]
    private let variants: [String: Locale]
    private var reserveCalls: [String] = []
    private var releaseCalls: [String] = []
    private var reserveGate: AssetGate?
    private var releaseGate: AssetGate?
    private var extraLocales: [Locale] = []

    init(initial: [Locale] = [], variants: [String: Locale] = [:]) {
        stored = Dictionary(initial.map { ($0.identifier(.bcp47), $0) },
                            uniquingKeysWith: { first, _ in first })
        self.variants = variants
    }

    nonisolated var client: SpeechAssetReservations.Inventory {
        .init(locales: { await self.locales() },
              reserve: { await self.reserve($0) },
              release: { await self.release($0) })
    }

    var snapshot: Snapshot {
        Snapshot(locales: Set(stored.keys), reserves: reserveCalls, releases: releaseCalls)
    }

    func holdReserve(at gate: AssetGate) { reserveGate = gate }
    func holdRelease(at gate: AssetGate) { releaseGate = gate }
    func addDuringNextReserve(_ locales: [Locale]) { extraLocales = locales }
    private func locales() -> [Locale] { Array(stored.values) }

    private func reserve(_ locale: Locale) async -> Bool {
        let requested = locale.identifier(.bcp47)
        reserveCalls.append(requested)
        await reserveGate?.wait()
        let actual = variants[requested] ?? locale
        let key = actual.identifier(.bcp47)
        guard stored[key] == nil else { return false }
        stored[key] = actual
        for extra in extraLocales { stored[extra.identifier(.bcp47)] = extra }
        extraLocales.removeAll()
        return true
    }

    private func release(_ locale: Locale) async -> Bool {
        let key = locale.identifier(.bcp47)
        releaseCalls.append(key)
        await releaseGate?.wait()
        return stored.removeValue(forKey: key) != nil
    }
}

@available(iOS 26.0, macOS 26.0, *)
@MainActor
private final class AssetReservationFixture {
    let inventory: AssetInventoryProbe
    let reservations: SpeechAssetReservations
    private var gates: [AssetGate] = []
    private var acquisitions: [Task<SpeechAssetReservations.Lease, Error>] = []
    private var work: [Task<Void, Error>] = []
    private var releases: [Task<Void, Never>] = []
    private var managers: [ModelManager] = []

    init(initial: [Locale] = [], variants: [String: Locale] = [:]) {
        let inventory = AssetInventoryProbe(initial: initial, variants: variants)
        self.inventory = inventory
        reservations = SpeechAssetReservations(inventory: inventory.client)
    }

    func gate() -> AssetGate {
        let gate = AssetGate()
        gates.append(gate)
        return gate
    }

    func acquire(_ locale: Locale) -> Task<SpeechAssetReservations.Lease, Error> {
        let task = Task { try await reservations.acquire(locale) }
        acquisitions.append(task)
        return task
    }

    func start(_ operation: @escaping @MainActor () async throws -> Void) -> Task<Void, Error> {
        let task = Task { try await operation() }
        work.append(task)
        return task
    }

    func release(_ lease: SpeechAssetReservations.Lease) -> Task<Void, Never> {
        let task = Task { await reservations.release(lease) }
        releases.append(task)
        return task
    }

    func manager(
        download: @escaping @Sendable ([any SpeechModule]) async throws -> Void
    ) -> ModelManager {
        let manager = ModelManager(reservations: reservations, assetDownloader: download)
        managers.append(manager)
        return manager
    }

    func waitUntil(_ predicate: () async -> Bool) async throws {
        let deadline = ContinuousClock.now + .seconds(5)
        while !(await predicate()) {
            guard ContinuousClock.now < deadline else { throw ProbeError.arrivalTimeout }
            try await Task.sleep(for: .milliseconds(1))
        }
    }

    func cleanup() async {
        for task in acquisitions { task.cancel() }
        for task in work { task.cancel() }
        for gate in gates { await gate.open() }
        for task in work { _ = await task.result }
        for manager in managers { await manager.releaseLocales() }
        for task in acquisitions {
            if case .success(let lease) = await task.result { await reservations.release(lease) }
        }
        for task in releases { await task.value }
    }

    enum ProbeError: Error { case arrivalTimeout, download }
}

@Suite("Speech asset reservations")
struct SpeechAssetReservationTests {
    @Test("concurrent sessions share acquisition and only the final owner releases")
    @MainActor
    func concurrentOwners() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture { fixture in
            let gate = fixture.gate()
            await fixture.inventory.holdReserve(at: gate)
            let first = fixture.acquire(Locale(identifier: "en-US"))
            try await fixture.waitUntil { await gate.entered }
            let second = fixture.acquire(Locale(identifier: "en-US"))
            await gate.open()
            let firstLease = try await first.value
            let secondLease = try await second.value
            await fixture.reservations.release(firstLease)
            var state = await fixture.inventory.snapshot
            #expect(state.reserves == ["en-US"])
            #expect(state.releases.isEmpty)
            await fixture.reservations.release(secondLease)
            state = await fixture.inventory.snapshot
            #expect(state.releases == ["en-US"])
            #expect(state.locales.isEmpty)
        }
    }

    @Test("release uses the observed native locale variant")
    @MainActor
    func canonicalVariant() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture(variants: ["en-AU": Locale(identifier: "en-US")]) { fixture in
            let lease = try await fixture.acquire(Locale(identifier: "en-AU")).value
            await fixture.reservations.release(lease)
            let state = await fixture.inventory.snapshot
            #expect(state.reserves == ["en-AU"])
            #expect(state.releases == ["en-US"])
            #expect(state.locales.isEmpty)
        }
    }

    @Test("preexisting variant reservations remain borrowed")
    @MainActor
    func preexistingReservation() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture(initial: [Locale(identifier: "en-US")],
                              variants: ["en-AU": Locale(identifier: "en-US")]) { fixture in
            let lease = try await fixture.acquire(Locale(identifier: "en-AU")).value
            await fixture.reservations.release(lease)
            let state = await fixture.inventory.snapshot
            #expect(state.releases.isEmpty)
            #expect(state.locales == ["en-US"])
        }
    }

    @Test("unknown borrowed aliases protect current candidates, not future unrelated assets")
    @MainActor
    func ambiguousBorrower() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture(variants: ["en-AU": Locale(identifier: "en-US")]) { fixture in
            let owner = try await fixture.acquire(Locale(identifier: "en-US")).value
            let borrower = try await fixture.acquire(Locale(identifier: "en-AU")).value
            let later = try await fixture.acquire(Locale(identifier: "ja-JP")).value
            await fixture.reservations.release(owner)
            var state = await fixture.inventory.snapshot
            #expect(state.releases.isEmpty)
            await fixture.reservations.release(later)
            state = await fixture.inventory.snapshot
            #expect(state.releases == ["ja-JP"])
            await fixture.reservations.release(borrower)
            state = await fixture.inventory.snapshot
            #expect(state.releases == ["ja-JP", "en-US"])
        }
    }

    @Test("ambiguous native mutations are not claimed or broadly released")
    @MainActor
    func ambiguousInventoryMutation() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture { fixture in
            await fixture.inventory.addDuringNextReserve([Locale(identifier: "fr-FR")])
            let operation = fixture.acquire(Locale(identifier: "en-US"))
            if case .success = await operation.result { Issue.record("Ambiguous inventory was accepted") }
            let state = await fixture.inventory.snapshot
            #expect(state.releases.isEmpty)
            #expect(state.locales == ["en-US", "fr-FR"])
        }
    }

    @Test("reacquisition during release waits and establishes a fresh reservation")
    @MainActor
    func reacquisitionDuringRelease() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture { fixture in
            let lease = try await fixture.acquire(Locale(identifier: "en-US")).value
            let gate = fixture.gate()
            await fixture.inventory.holdRelease(at: gate)
            let release = fixture.release(lease)
            try await fixture.waitUntil { await gate.entered }
            let next = fixture.acquire(Locale(identifier: "en-US"))
            await gate.open()
            await release.value
            let nextLease = try await next.value
            var state = await fixture.inventory.snapshot
            #expect(state.reserves == ["en-US", "en-US"])
            #expect(state.locales == ["en-US"])
            await fixture.reservations.release(nextLease)
            state = await fixture.inventory.snapshot
            #expect(state.releases == ["en-US", "en-US"])
        }
    }

    @Test("manager download failure returns its explicitly acquired locale")
    @MainActor
    func downloadFailure() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture { fixture in
            let module = SpeechTranscriber(locale: Locale(identifier: "en-US"), preset: .transcription)
            let manager = fixture.manager { _ in throw AssetReservationFixture.ProbeError.download }
            let operation = fixture.start { try await manager.ensureAssets(for: [module]) }
            if case .success = await operation.result { Issue.record("Download failure was ignored") }
            let state = await fixture.inventory.snapshot
            #expect(!state.reserves.isEmpty)
            #expect(state.releases.count == state.reserves.count)
            #expect(state.locales.isEmpty)
        }
    }

    @Test("canceling manager acquisition cannot abandon a late native reservation")
    @MainActor
    func canceledAcquisition() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture { fixture in
            let gate = fixture.gate()
            await fixture.inventory.holdReserve(at: gate)
            let module = SpeechTranscriber(locale: Locale(identifier: "en-US"), preset: .transcription)
            let manager = fixture.manager { _ in Issue.record("Canceled acquisition started a download") }
            let operation = fixture.start { try await manager.ensureAssets(for: [module]) }
            try await fixture.waitUntil { await gate.entered }
            operation.cancel()
            await gate.open()
            if case .success = await operation.result { Issue.record("Canceled acquisition succeeded") }
            let state = await fixture.inventory.snapshot
            #expect(state.releases.count == 1)
            #expect(state.locales.isEmpty)
        }
    }

    @Test("manager cleanup drains pending acquisitions before returning")
    @MainActor
    func cleanupDuringAcquisition() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture { fixture in
            let gate = fixture.gate()
            await fixture.inventory.holdReserve(at: gate)
            let module = SpeechTranscriber(locale: Locale(identifier: "en-US"), preset: .transcription)
            let manager = fixture.manager { _ in Issue.record("Retired preparation started a download") }
            let operation = fixture.start { try await manager.ensureAssets(for: [module]) }
            try await fixture.waitUntil { await gate.entered }
            let cleanup = fixture.start { await manager.releaseLocales() }
            try await fixture.waitUntil { await manager.cleanupTask != nil }
            await gate.open()
            _ = await operation.result
            try await cleanup.value
            let state = await fixture.inventory.snapshot
            #expect(state.releases.count == 1)
            #expect(state.locales.isEmpty)
            #expect(await manager.cleanupTask == nil)
        }
    }

    @Test("manager cleanup retains reservations until an active download returns")
    @MainActor
    func cleanupDuringDownload() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture { fixture in
            let gate = fixture.gate()
            let module = SpeechTranscriber(locale: Locale(identifier: "en-US"), preset: .transcription)
            let manager = fixture.manager { _ in await gate.wait() }
            let operation = fixture.start { try await manager.ensureAssets(for: [module]) }
            try await fixture.waitUntil { await gate.entered }
            let cleanup = fixture.start { await manager.releaseLocales() }
            try await fixture.waitUntil { await manager.cleanupTask != nil }
            var state = await fixture.inventory.snapshot
            #expect(state.releases.isEmpty)
            await gate.open()
            if case .success = await operation.result { Issue.record("Retired download was admitted") }
            try await cleanup.value
            state = await fixture.inventory.snapshot
            #expect(state.releases.count == 1)
            #expect(state.locales.isEmpty)
        }
    }

    @available(iOS 26.0, macOS 26.0, *)
    @MainActor
    private func withFixture(
        initial: [Locale] = [], variants: [String: Locale] = [:],
        operation: @MainActor (AssetReservationFixture) async throws -> Void
    ) async throws {
        let fixture = AssetReservationFixture(initial: initial, variants: variants)
        do {
            try await operation(fixture)
        } catch {
            await fixture.cleanup()
            throw error
        }
        await fixture.cleanup()
    }
}
