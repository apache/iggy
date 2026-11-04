/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iggy.message;

import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyValidationException;
import org.apache.iggy.partition.PartitionContext;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PollOptionsTest {
    private static final PartitionContext CONTEXT =
            new PartitionContext(BigInteger.valueOf(3), BigInteger.valueOf(4), BigInteger.valueOf(5));

    @Test
    void shouldAnswerAtOnceAndStoreNothingByDefault() {
        assertThat(PollOptions.defaults())
                .isEqualTo(new PollOptions(1000, Duration.ZERO, 1, 1024 * 1024, CommitPolicy.manual()));
        assertThat(PollOptions.defaults().requestTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThatCode(() -> PollOptions.defaults().validate()).doesNotThrowAnyException();
    }

    @Test
    void shouldStoreEvery10000MessagesOr5SecondsAfterProcessingByDefault() {
        assertThat(AutoCommit.defaults())
                .isEqualTo(new AutoCommit(
                        Optional.of(Duration.ofSeconds(5)),
                        Optional.of(CommitEvery.messages(10_000)),
                        CommitAfter.PROCESSING));
        assertThat(CommitPolicy.auto()).isEqualTo(CommitPolicy.auto(AutoCommit.defaults()));
    }

    @Test
    void shouldRefuseAnAutomaticCommitThatWouldStoreOnlyOnClose() {
        var never = PollOptions.defaults()
                .withCommit(
                        CommitPolicy.auto(new AutoCommit(Optional.empty(), Optional.empty(), CommitAfter.DELIVERY)));

        assertThatThrownBy(never::validate)
                .isInstanceOfSatisfying(
                        IggyValidationException.class,
                        error -> assertThat(error.getErrorCode()).isEqualTo(IggyErrorCode.INVALID_CONFIGURATION));
        assertThatCode(() -> PollOptions.defaults()
                        .withCommit(CommitPolicy.auto(AutoCommit.defaults().withoutInterval()))
                        .validate())
                .doesNotThrowAnyException();
        assertThatCode(() -> PollOptions.defaults()
                        .withCommit(CommitPolicy.auto(AutoCommit.defaults().withoutEvery()))
                        .validate())
                .doesNotThrowAnyException();
    }

    @Test
    void shouldRefuseLimitsThatTheServerRefuses() {
        assertThatThrownBy(() -> PollOptions.defaults().withCount(0).validate())
                .isInstanceOf(IggyInvalidArgumentException.class);
        assertThatThrownBy(() -> PollOptions.defaults().withMinCount(1001).validate())
                .isInstanceOf(IggyInvalidArgumentException.class);
        assertThatThrownBy(() -> PollOptions.waitFor(Duration.ofSeconds(601)).validate())
                .isInstanceOf(IggyInvalidArgumentException.class);
        assertThatThrownBy(() -> PollOptions.defaults().withMaxBytes(1).validate())
                .isInstanceOf(IggyInvalidArgumentException.class);
        assertThatThrownBy(() -> CommitEvery.messages(0)).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void shouldWaitTheRequestTimeoutTenSecondsLongerThanTheWait() {
        var options = PollOptions.waitFor(Duration.ofSeconds(5));

        assertThat(options.requestTimeout()).isEqualTo(Duration.ofSeconds(15));
        assertThat(options.deferredOptions())
                .isEqualTo(new DeferredPollOptions(Duration.ofSeconds(5), 1, 1024 * 1024, Duration.ofSeconds(15)));
    }

    @Test
    void shouldBindTheCheckpointAndTheContinuationOfABatchToItsPartition() {
        var batch = new PolledMessages(2L, BigInteger.TEN, 2L, List.of(message(7), message(8)), CONTEXT);

        Checkpoint last = batch.checkpoint().orElseThrow();

        assertThat(List.of(last.partitionId(), last.offset(), last.context()))
                .containsExactly(2L, BigInteger.valueOf(8), CONTEXT);
        assertThat(batch.checkpointAt(0).map(Checkpoint::offset)).contains(BigInteger.valueOf(7));
        assertThat(batch.checkpointAt(2)).isEmpty();
        assertThat(batch.continuation())
                .contains(new PollingStrategy(
                        PollingKind.Offset, BigInteger.valueOf(9), Optional.of(CONTEXT), Optional.of(2L)));
        assertThatCode(() -> batch.continuation().orElseThrow().requirePartition(2))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> batch.continuation().orElseThrow().requirePartition(3))
                .isInstanceOfSatisfying(
                        IggyValidationException.class,
                        error -> assertThat(error.getErrorCode()).isEqualTo(IggyErrorCode.INVALID_COMMAND));
        assertThat(new PolledMessages(2L, BigInteger.TEN, 0L, List.of(), CONTEXT).continuation())
                .isEmpty();
    }

    @Test
    void shouldKeepThePartitionOfAContinuationWhenItsContextChanges() {
        var continuation = new PolledMessages(2L, BigInteger.TEN, 1L, List.of(message(7)), CONTEXT)
                .continuation()
                .orElseThrow()
                .withContext(PartitionContext.EMPTY);

        assertThat(continuation.partitionId()).contains(2L);
    }

    private static Message message(long offset) {
        var plain = Message.of("payload");
        var header = new MessageHeader(
                BigInteger.ZERO,
                plain.header().id(),
                BigInteger.valueOf(offset),
                BigInteger.ZERO,
                BigInteger.ZERO,
                0L,
                (long) plain.payload().length,
                BigInteger.ZERO);
        return new Message(header, plain.payload(), plain.userHeaders());
    }
}
