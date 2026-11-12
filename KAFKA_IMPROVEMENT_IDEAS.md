# Apache Kafka - Minor Improvements & Unassigned Issues

**Generated:** 2026-06-02  
**Source:** Apache Kafka JIRA Analysis

This document catalogs minor improvements, unassigned issues, and test enhancements that could be good candidates for contribution to Apache Kafka, with a focus on share groups and test improvements.

---

## 🎯 Share Group Issues

### KAFKA-20647: Add Example for Queues/Share Groups
- **Priority:** Minor
- **Status:** UNASSIGNED
- **Type:** Task
- **URL:** https://issues.apache.org/jira/browse/KAFKA-20647

**Description:**  
Create a basic example demonstrating Queues for Kafka (Share Groups) usage. Share groups became production-ready in Kafka 4.2+, but the examples module lacks demonstration code.

**What Needs to Be Done:**
- Write example code showing fundamental share group operations
- Add comments explaining key concepts
- Include in examples module

**Impact:** Developer onboarding, documentation completeness  
**Effort:** Low - example code with documentation

---

### KAFKA-20585: Share Fetch Latency Metric Incorrectly Updated
- **Priority:** Major
- **Status:** Assigned to Muralidhar Basani
- **Type:** Improvement
- **URL:** https://issues.apache.org/jira/browse/KAFKA-20585

**Description:**  
ShareFetchMetricsManager has two issues with latency metric recording:

1. **Incorrect Recording Location:** Metrics currently recorded in both ShareFetch AND ShareAcknowledge response handlers. Should only be in ShareFetch responses.
2. **Missing Error Responses:** Metrics recorded at end of response handler, so top-level error responses aren't captured in share-fetch-rate and share-fetch-total metrics.

**What Needs to Be Done:**
- Move metric recording exclusively to ShareFetch response handler
- Ensure top-level errors are captured in metrics
- Similar pattern exists in FetchManager (investigate if intentional)

**Impact:** Accurate observability for share groups  
**Effort:** Medium - requires understanding metric recording flow

---

### KAFKA-20095: Unnecessary Triggers to Delayed Share Fetch Purgatory
- **Priority:** Major
- **Status:** Assigned to Abhinav Dixit
- **Type:** Bug
- **URL:** https://issues.apache.org/jira/browse/KAFKA-20095
- **Affects Version:** Kafka 4.2.0

**Description:**  
Elevated CPU consumption when consuming from a large number of share-partitions with no data being fetched. Occurs specifically when production has halted and consumer is idle waiting for new records.

**Problem:**  
Purgatory repeatedly triggered during idle periods even though no data is being retrieved, creating unnecessary computational overhead.

**What Needs to Be Done:**
- Refine delayed fetch purgatory logic to avoid spurious triggers
- Optimize resource utilization during production inactivity periods
- Test with large number of share-partitions

**Impact:** CPU efficiency for share groups at scale  
**Effort:** High - performance optimization

---

### KAFKA-20070: Evaluate Upper Bounds for share.coordinator.snapshot Parameters
- **Priority:** Major
- **Status:** Assigned to Muralidhar Basani
- **Type:** Improvement
- **Labels:** need-kip
- **URL:** https://issues.apache.org/jira/browse/KAFKA-20070

**Description:**  
Define reasonable upper bounds for `share.coordinator.snapshot.update.records.per.snapshot` configuration parameter.

**What Needs to Be Done:**
- Determine appropriate maximum thresholds through performance testing
- Prevent performance degradation or resource exhaustion
- Author KIP documenting the bounds and rationale
- Balance snapshot frequency with system performance

**Impact:** Production stability, configuration guidance  
**Effort:** High - requires KIP authoring and extensive testing

---

### KAFKA-20643: Share Common Implementation for Downstream Offset Translation
- **Priority:** Minor
- **Status:** UNASSIGNED
- **Type:** Improvement
- **URL:** https://issues.apache.org/jira/browse/KAFKA-20643

**Description:**  
MirrorMaker's offset sync logic has inconsistency. `OffsetSyncWriter.PartitionState` uses formula `downstreamOffset - (lastSyncDownstreamOffset + 1) >= maxOffsetLag` where "+1" corresponds to translation in `OffsetSyncStore.translateDownstream`. Currently enforced only through comment/TODO.

**Problem:**  
If translation logic changes in `OffsetSyncStore` without matching updates in `OffsetSyncWriter`, offset staleness detection becomes unreliable.

**What Needs to Be Done:**
- Extract downstream translation boundary calculation into common helper function
- Use consistently across `OffsetSyncStore.translateDownstream` and `OffsetSyncWriter.PartitionState.update`
- Remove TODO comments
- Add tests

**Impact:** Code maintainability, prevents bugs in offset staleness detection  
**Effort:** Medium - refactoring with comprehensive tests

---

## 🧪 Test Improvements (Unassigned)

### KAFKA-20454: Verify kafka-consumer-groups.sh Fails for Streams Groups
- **Priority:** Minor
- **Status:** UNASSIGNED
- **Type:** Test
- **Component:** Streams, Unit tests
- **URL:** https://issues.apache.org/jira/browse/KAFKA-20454

**Description:**  
KIP-848/1071 created three distinct group types: "classic," "consumer," and "streams." The `kafka-consumer-groups.sh` tool exclusively uses "classic" group type. While mixed-mode works for consumer groups, it's NOT supported for streams groups.

**What Needs to Be Done:**
- Add test verifying `--reset-offsets` command fails appropriately for streams group
- Ensure proper error message is returned
- Prevent silent failures with incompatible mixed-mode behavior

**Impact:** Validate tool behavior, prevent silent failures  
**Effort:** Low - single test case

---

### KAFKA-19913: Increase Code Coverage of VerifiableProperties Class
- **Priority:** Minor
- **Status:** UNASSIGNED
- **Type:** Test
- **Component:** Core, Unit tests
- **URL:** https://issues.apache.org/jira/browse/KAFKA-19913
- **Related PR:** #20984

**Description:**  
VerifiableProperties class has very low unit test coverage: **12.93%**

**What Needs to Be Done:**
- Add comprehensive unit tests for VerifiableProperties class
- Cover all public methods
- Test edge cases and error conditions
- Aim for >80% coverage

**Impact:** Code quality, prevent regressions in critical class  
**Effort:** Medium - comprehensive test cases needed

---

### KAFKA-19308: Improve OAuth Unit Testing Based on Code Coverage
- **Priority:** Minor
- **Status:** UNASSIGNED
- **Type:** Test
- **URL:** https://issues.apache.org/jira/browse/KAFKA-19308
- **Related PRs:** #20454, #21889

**Description:**  
Increase unit testing of OAuth layer for improved code coverage.

**What Needs to Be Done:**
- Identify OAuth components lacking sufficient test coverage
- Implement test scenarios for authentication flows
- Test error conditions and edge cases
- Document coverage improvements

**Impact:** Security confidence, regression prevention  
**Effort:** Medium - OAuth flow testing is complex

---

### KAFKA-15403: Refactor @Test(expected) to assertThrows
- **Priority:** Minor
- **Status:** UNASSIGNED
- **Type:** Test
- **URL:** https://issues.apache.org/jira/browse/KAFKA-15403
- **Related PR:** #14264

**Description:**  
Migrate deprecated `@Test(expected)` annotation to modern `assertThrows` method for exception handling in unit tests.

**Current Approach:**
```java
@Test(expected = SomeException.class)
public void testMethod() {
    // test code
}
```

**Improved Approach:**
```java
@Test
public void testMethod() {
    assertThrows(SomeException.class, () -> {
        // test code
    });
}
```

**Benefits:**
- **Precision:** Assert on specific exception messages or properties
- **Readability:** Exception handling explicit within test
- **Flexibility:** Multiple exception assertions possible in single test

**What Needs to Be Done:**
- Identify all uses of `@Test(expected)`
- Migrate to `assertThrows`
- Verify tests still pass
- Consider adding message assertions where valuable

**Impact:** Test code quality, maintainability  
**Effort:** Low per test, but many instances across codebase

---

## 📝 General Improvements (Unassigned)

### KAFKA-20652: Fix JavaDoc Issues
- **Priority:** Minor
- **Status:** UNASSIGNED (PR #22455 submitted)
- **Type:** Improvement
- **Component:** Streams
- **Target Version:** 4.4.0
- **URL:** https://issues.apache.org/jira/browse/KAFKA-20652

**Description:**  
Three straightforward javadoc corrections needed in Kafka Streams:

1. **Produced.java (line 117):** Parameter description says "@param valueSerde Serde to use for serializing the key" - should reference "value" not "key"

2. **Four identical typos:**
   - Files: WindowStore.java, ReadOnlyWindowStore.java (2 instances), SegmentedBytesStore.java
   - Fix: "within in the given time range" → "within the given time range"

3. **JoinWindows.java (line 157):** Stray double closing brace `}}` in Javadoc link tag → reduce to single brace

**What Needs to Be Done:**
- Fix parameter description in Produced.java
- Remove duplicate "in" from 4 files
- Fix double brace in JoinWindows.java
- Verify javadoc renders correctly

**Impact:** Documentation accuracy  
**Effort:** Very Low - ~6 lines total

---

### KAFKA-20646: Use Locale.ROOT in String.format() Calls
- **Priority:** Minor
- **Status:** UNASSIGNED
- **Type:** Improvement
- **Modules:** tools/, server-common/
- **URL:** https://issues.apache.org/jira/browse/KAFKA-20646

**Description:**  
~75 instances of `String.format()` across 29 files need to use `Locale.ROOT` to ensure consistent, locale-independent behavior.

**Problem:**  
`String.format()` without Locale parameter produces locale-dependent output:
- Different decimal separators for numbers
- Locale-specific formatting variations

**Example Change:**
```java
// Before
String.format("Value: %.2f", value)

// After
String.format(Locale.ROOT, "Value: %.2f", value)
```

**What Needs to Be Done:**
- Update ~75 instances: `String.format(...)` → `String.format(Locale.ROOT, ...)`
- Files in tools/ and server-common/ modules
- Verify existing tests pass
- Consider adding test for locale independence

**Impact:** Cross-environment consistency, prevents locale bugs  
**Effort:** Low - systematic find/replace with verification

**Context:**  
Follow-up to similar locale issue in ConsumerGroupCommand. Aligns with existing checkstyle rules enforcing Locale.ROOT for text case conversions.

---

### KAFKA-20568: Unify DSL Store Accessor and Builder Naming
- **Priority:** Minor
- **Status:** UNASSIGNED
- **Type:** Improvement
- **Component:** Streams
- **URL:** https://issues.apache.org/jira/browse/KAFKA-20568

**Description:**  
Two naming inconsistencies in Kafka Streams library:

1. **DSL Store Format Accessor:**
   - Some code: `dslStoreFormat()`
   - Other code: `storeFormat()`
   - Need to pick one

2. **Header-Aware Builder Classes:**
   - Some classes: `*WithHeadersBuilder` pattern
   - Other classes: `*BuilderWithHeaders` pattern
   - Need to standardize

**What Needs to Be Done:**
- Establish unified naming standard for both patterns
- Update all relevant code to match standard
- Update documentation
- Ensure API consistency across Streams library

**Impact:** API consistency, developer experience  
**Effort:** Medium - requires API design decision + coordinated updates

---

### KAFKA-20537: Return topic-id in CreateTopicsResponse When Topic Exists
- **Priority:** Minor
- **Status:** UNASSIGNED
- **Type:** Improvement
- **URL:** https://issues.apache.org/jira/browse/KAFKA-20537

**Description:**  
When CREATE_TOPICS request includes a topic that already exists, Kafka returns "TOPIC_ALREADY_EXISTS" error without including the topic ID.

**Problem:**  
The topic ID already exists for the pre-existing topic, so it should be included in the response even when returning an error.

**What Needs to Be Done:**
- Modify CreateTopicsResponse to populate topicId field in error cases
- Specifically for TOPIC_ALREADY_EXISTS error
- Update tests
- Update documentation/protocol docs

**Impact:** API completeness, client convenience  
**Effort:** Low - simple response enhancement

---

### KAFKA-20520: Make Config Deletion Idempotent
- **Priority:** Minor
- **Status:** UNASSIGNED (PR discussion in progress)
- **Type:** Improvement
- **Tool:** kafka-configs.sh
- **URL:** https://issues.apache.org/jira/browse/KAFKA-20520
- **Reporter/Assignee:** Ken Huang

**Description:**  
Config deletion operations in `kafka-configs.sh` should be consistent and idempotent.

**Problem:**  
Executing the same deletion command multiple times may cause errors instead of being a no-op.

**What Needs to Be Done:**
- Refine config deletion functionality
- Ensure repeated deletion attempts produce same result without errors
- Make behavior consistent with expectations
- Add tests for idempotency

**Impact:** Operational safety, better tool UX  
**Effort:** Low-Medium - tool behavior fix

---

### KAFKA-17798: Add Forbidden-APIs Linting to Gradle Build
- **Priority:** Minor
- **Status:** UNASSIGNED
- **Type:** Improvement
- **Related PRs:** #17565, #21097
- **URL:** https://issues.apache.org/jira/browse/KAFKA-17798

**Description:**  
Integrate forbidden-apis tool into Kafka's Gradle build for bytecode-level validation of API usage.

**What Needs to Be Done:**
- Add forbidden-apis project to Gradle build system
- Incorporate checks into CI pipeline
- Start with minimal/no restrictions initially
- Gradually enable specific API restrictions in separate issues

**Follow-up Issues (Blocked by this):**
- **KAFKA-17799:** Prevent use of default system locale
- **KAFKA-17800:** Forbid `System.exit()` and `Runtime.exit()` calls

**Impact:** Code quality, prevents bugs at build time  
**Effort:** Medium - build infrastructure work, ongoing maintenance

---

## 📊 Quick Reference Matrix

### By Effort Level

**Very Low Effort:**
- KAFKA-20652: Fix JavaDoc issues (~10 minutes)
- KAFKA-20454: Add streams group test

**Low Effort:**
- KAFKA-20646: Locale.ROOT in String.format()
- KAFKA-20537: Return topic-id in error response
- KAFKA-20520: Idempotent config deletion
- KAFKA-20647: Add share groups example

**Medium Effort:**
- KAFKA-20643: Share offset translation implementation
- KAFKA-20568: Unify DSL naming
- KAFKA-19913: VerifiableProperties coverage (12.93% → 80%+)
- KAFKA-19308: OAuth testing improvements
- KAFKA-17798: Forbidden-APIs linting
- KAFKA-15403: Migrate @Test(expected) to assertThrows

**High Effort:**
- KAFKA-20585: Share fetch latency metrics
- KAFKA-20095: Purgatory trigger optimization
- KAFKA-20070: Snapshot parameter bounds (needs KIP)

---

### By Domain

**Share Groups:**
- KAFKA-20647 (example)
- KAFKA-20585 (metrics)
- KAFKA-20095 (purgatory)
- KAFKA-20070 (snapshots)
- KAFKA-20643 (offset translation)

**Testing:**
- KAFKA-20454 (streams group test)
- KAFKA-19913 (VerifiableProperties coverage)
- KAFKA-19308 (OAuth testing)
- KAFKA-15403 (assertThrows migration)

**Documentation:**
- KAFKA-20652 (JavaDoc fixes)
- KAFKA-20647 (share groups example)

**Code Quality:**
- KAFKA-20646 (Locale.ROOT)
- KAFKA-20568 (naming consistency)
- KAFKA-17798 (forbidden-APIs)

**Tools:**
- KAFKA-20520 (idempotent deletion)
- KAFKA-20454 (CLI validation)

**API:**
- KAFKA-20537 (topic-id in response)

---

## 🎯 Recommended Starting Points

### For Share Groups Expertise:
1. **KAFKA-20647** - Add share groups example (great for visibility!)
2. **KAFKA-20585** - Understand metrics issues (even if assigned)
3. **KAFKA-20095** - Understand purgatory issues (performance critical)

### For Quick Wins:
1. **KAFKA-20652** - JavaDoc fixes (10 minutes, easy PR)
2. **KAFKA-20646** - Locale.ROOT changes (systematic, good automation candidate)
3. **KAFKA-20454** - Add CLI test (straightforward test case)

### For Test Infrastructure:
1. **KAFKA-19913** - VerifiableProperties (only 12.93% coverage!)
2. **KAFKA-15403** - Modernize test assertions (codebase-wide improvement)
3. **KAFKA-19308** - OAuth testing (security-critical)

---

## 📚 Additional Context

### Recent Releases
- **Kafka 4.2.0** (February 2026): Share groups production-ready, RENEW acknowledgment, adaptive batching
- **Kafka 4.3.0** (May 2026): Share groups operational tuning (KIP-1240), broker/group-level configs

### Useful Links
- Apache Kafka JIRA: https://issues.apache.org/jira/browse/KAFKA
- Kafka Dev Mailing List: dev@kafka.apache.org
- Contributing Guide: https://kafka.apache.org/contributing
- KIP Process: https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals

---

**Last Updated:** 2026-06-02  
**Maintainer:** Apoorv Mittal (amittal@confluent.io)
