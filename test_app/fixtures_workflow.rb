# frozen_string_literal: true

# Twins of the reference's durable structured workflow fixtures
# (test/apps/rust_runner/src/workflow.rs and workflow_loop.rs) under the same
# factory ids, tasklet names, channel names and output bytes.
#
# Entry payloads and plugin frames are UTF-8 text on the wire, as in the
# reference: every entry names the text codec, and a bare `.sink(name)` sends a
# String as UTF-8 here as it does there. An inter-tasklet hop between two Ruby
# tasklets carries MessagePack, and the core derives that channel's journal
# codec from the channel's kind, so no fixture declares one.

require "flezha"

module Fixtures
  # -- the release gate -----------------------------------------------------

  RELEASE_ENV = "FLEZHA_WORKFLOW_E2E_RELEASE"

  # Nothing ever pushes to this queue: it is the park itself, not a handoff.
  RELEASE_GATE = Thread::Queue.new

  # Hold this activation until a runner boots with the release variable set.
  #
  # The harness kills the first runner and launches its replacement with the
  # variable present, so the gate is a boot-time decision: never answering on
  # the first runner, returning at once on the second.
  #
  # A queue rather than a timed wait, for two reasons. `Thread::Queue#pop` is
  # scheduler-aware, so one spelling parks a pool worker under the thread
  # executor and yields the fiber under the fiber one — the runner keeps
  # answering control pings either way, and a sibling tasklet stays
  # dispatchable. And it holds no timer, so nothing here polls.
  def self.block_until_released
    RELEASE_GATE.pop unless ENV.key?(RELEASE_ENV)
  end

  # Insert into a sorted, de-duplicated array.
  #
  # The reference's `SeenState` is a `BTreeSet<String>`, chosen because a set
  # keeps snapshot bytes deterministic (workflow.rs:50). A Set has no durable
  # form here — the codec refuses one where the state is declared — so the twin
  # is the array it would decode to, kept sorted for the same determinism and
  # idempotent on insert for the same re-run after a resume.
  def self.insert_sorted(items, value)
    at = items.index { |item| item >= value }
    return items.push(value) if at.nil?

    items.insert(at, value) unless items[at] == value
    items
  end

  # -- the two shared tasklet shapes --------------------------------------

  # The reference's `durable_step!` macro (workflow.rs:66): a String -> String
  # step committing one idempotent Change keyed by its own name, so a re-run
  # after a resume folds to the state it already had.
  #
  # The macro parameterises the emit and the compensation. `recorded` and
  # `gated?` are the two axes the reference varies by hand-writing the
  # tasklet instead — the value journaled, and whether the step waits for the
  # release gate BEFORE recording it. Carrying all four as overridable methods
  # is what lets every durable tasklet here be a subclass with one line.
  class DurableStep < Flezha::Tasklet
    def initial_state(_cell_key) = { items: [] }
    def apply_change(state, change) = Fixtures.insert_sorted(state[:items], change[:insert])

    # The value this step journals, or nil for none.
    def recorded(_input) = self.class.tasklet_name
    # Whether it waits for the gate before journaling anything.
    def gated?(_input) = false
    # What it releases. A value is emitted by the dispatcher; a step needing a
    # key emits through the ctx and answers nil.
    def released(input, _ctx) = input

    def run(input, ctx)
      Fixtures.block_until_released if gated?(input)
      value = recorded(input)
      ctx.change({ insert: value }) unless value.nil?
      released(input, ctx)
    end
  end

  # A durable step that compensates by emitting one undo record. A
  # crash-resumed compensation reconstructs completed stages with EMPTY
  # outputs, so the hook records its undo unconditionally.
  class UndoingStep < DurableStep
    def undo_record = "undone"
    def compensate(_outputs, ctx) = ctx.emit(undo_record)
  end

  # -- echo_workflow --------------------------------------------------------
  #
  # One tasklet, run once (workflow.rs): its single pass runs on the first
  # message and emits it back, so the pass ends and the controller
  # deregisters the instance.

  class Echo < Flezha::Tasklet
    tasklet_name "echo"
    def run(input, _ctx) = "echo:#{input}"
  end

  class EchoWorkflow < Flezha::WorkflowOperative
    factory_id "echo_workflow"
    channels { |c| c.input("in", codec: :text).into(Echo).sink("out") }
  end

  # -- typed_once_workflow --------------------------------------------------
  #
  # A `once` workflow whose entry decodes JSON: its one pass runs over the
  # message it was spawned or sent with, prints what it read, emits it to the
  # `placed` sink and completes with it.

  class OncePlaced < Flezha::Tasklet
    tasklet_name "once_placed"

    def run(input, ctx)
      text = input.fetch(:text).to_s
      puts("once_ran #{text}")
      $stdout.flush
      ctx.complete(text)
      "placed:#{text}"
    end
  end

  class TypedOnceWorkflow < Flezha::WorkflowOperative
    factory_id "typed_once_workflow"
    channels { |c| c.input("order", codec: :json).into(OncePlaced).sink("placed") }
  end

  # -- commit_block_workflow / sync_durable_workflow ------------------------
  #
  # Task A commits durable state, task B blocks until the resumed runner sets
  # the release variable — so a SIGKILL lands with A's state already on the WAL
  # and B never finished. `sync_durable_workflow` is the same chain declaring
  # SYNCHRONOUS remote durability, which is what lets the kill-the-disk drill
  # destroy the state directory and still resume with A committed.

  class CommitA < DurableStep
    tasklet_name "commit_a"
  end

  class BlockB < Flezha::Tasklet
    tasklet_name "block_b"
    def run(_input, _ctx) = Fixtures.block_until_released
  end

  COMMIT_BLOCK_CHAIN = lambda do |c|
    c.input("in", codec: :text).into(CommitA).named("mid").into(BlockB)
  end

  class CommitBlockWorkflow < Flezha::WorkflowOperative
    factory_id "commit_block_workflow"
    channels(&COMMIT_BLOCK_CHAIN)
  end

  class SyncDurableWorkflow < Flezha::WorkflowOperative
    factory_id "sync_durable_workflow"
    sync_durability
    channels(&COMMIT_BLOCK_CHAIN)
  end

  # -- optout_commit_block --------------------------------------------------
  #
  # The opt-out twin with a live mid-chain signal: task A's output also reaches
  # a plugin sink, so the crash-resume drill can observe the mid-run point
  # without durable state. A crash re-runs the chain from the top, so the sink
  # sees A's marker once per run and no journal file exists.

  class OptoutCommitBlock < Flezha::WorkflowOperative
    factory_id "optout_commit_block"
    persistent false
    channels do |c|
      c.input("in", codec: :text).into(CommitA)
       .sink("mid", encode: ->(value) { "a-ran:#{value}" })
       .into(BlockB)
    end
  end

  # -- tier_commit_block_workflow -------------------------------------------
  #
  # The keyed hybrid twin. Task A commits durable state AND emits
  # `committed:<input>` under a stable key on a keyed hybrid channel; task B
  # blocks. A SIGKILL lands with A's stage committed and B unfinished, and the
  # resume re-runs B alone.

  class TierCommitA < DurableStep
    tasklet_name "tier_commit_a"
    def released(input, ctx) = ctx.emit("committed:#{input}", key: "job")
  end

  class TierCommitBlockWorkflow < Flezha::WorkflowOperative
    factory_id "tier_commit_block_workflow"
    channels do |c|
      c.input("in", codec: :text).into(TierCommitA).keyed
       .sink("mid")
       .into(BlockB)
    end
  end

  # -- saga_block_workflow --------------------------------------------------
  #
  # SA and SB commit durable state and compensate; SC fails, triggering
  # reverse-order rollback (SB then SA). SA's compensation BLOCKS, so a SIGKILL
  # lands mid-rollback with SB's done-marker already durable — the respawn
  # finishes SA and SKIPS the already-done SB.

  class SagaA < DurableStep
    tasklet_name "saga_a"
    def compensate(_outputs, _ctx) = Fixtures.block_until_released
  end

  class SagaB < DurableStep
    tasklet_name "saga_b"
    def compensate(_outputs, _ctx) = nil
  end

  class SagaC < Flezha::Tasklet
    tasklet_name "saga_c"
    def run(_input, _ctx) = raise("saga_c always fails")
  end

  class SagaBlockWorkflow < Flezha::WorkflowOperative
    factory_id "saga_block_workflow"
    channels do |c|
      c.input("in", codec: :text).into(SagaA).named("s1")
       .into(SagaB).named("s2")
       .into(SagaC)
    end
  end

  # -- keyed_block_workflow / keyed_saga_workflow ---------------------------
  #
  # The entry's comma-separated key list is split into one value per key, and
  # a `key_by` on that value mints one Cell per key of a durable compensable
  # tasklet that declares no keys; each Cell commits one idempotent change. The
  # third tasklet blocks until the release variable, or fails, so the keyed
  # stage is rolled back once per key.

  class KeySplit < Flezha::Tasklet
    tasklet_name "key_split"

    def run(input, ctx)
      input.split(",").each { |key| ctx.emit(key) }
      nil
    end
  end

  class KeyedCommit < DurableStep
    tasklet_name "keyed_commit"
    dynamic_cells
    def compensate(_outputs, _ctx) = nil
  end

  class KeyedFail < Flezha::Tasklet
    tasklet_name "keyed_fail"
    def run(_input, _ctx) = raise("keyed_fail always fails")
  end

  KEYED_COMMITS = lambda do |c|
    c.input("in", codec: :text).into(KeySplit).named("keys")
     .key_by { |key| key }
     .into(KeyedCommit).named("committed")
  end

  class KeyedBlockWorkflow < Flezha::WorkflowOperative
    factory_id "keyed_block_workflow"
    channels { |c| KEYED_COMMITS.call(c).into(BlockB) }
  end

  class KeyedSagaWorkflow < Flezha::WorkflowOperative
    factory_id "keyed_saga_workflow"
    channels { |c| KEYED_COMMITS.call(c).into(KeyedFail) }
  end

  # -- outbox_block_workflow ------------------------------------------------
  #
  # The transactional outbox. Task A commits durable state AND emits
  # plugin-bound output on a hybrid channel — STAGED, so it reaches the sink
  # only after the controller has journaled and fsynced A's committed entry
  # with the output inside it and flushed the outbox. Task B blocks, so a
  # SIGKILL lands with A committed and flushed and B unfinished.

  class OutboxBlockWorkflow < Flezha::WorkflowOperative
    factory_id "outbox_block_workflow"
    channels do |c|
      c.input("in", codec: :text).into(CommitA)
       .sink("mid", encode: ->(value) { "a-out:#{value}" })
       .into(BlockB)
    end
  end

  # -- undo_outbox_workflow -------------------------------------------------
  #
  # The compensation half of the transactional outbox. Task UA commits, emits
  # its forward output on a hybrid plugin channel, and compensates by EMITTING
  # an undo record — plugin-bound, so it is staged and rides a compensated
  # journal entry rather than the wire. Task C fails immediately, so the chain
  # always rolls back; the hook BLOCKS, so on the first runner the undo is
  # emitted-but-never-journaled when the SIGKILL lands, proving staged undo
  # output cannot leak. The respawn re-runs the outstanding hook, whose done
  # marker never landed, and the undo is delivered exactly once.

  class UndoOutboxA < DurableStep
    tasklet_name "undo_outbox_a"

    def compensate(_outputs, ctx)
      Fixtures.block_until_released
      ctx.emit("undone")
    end
  end

  class UndoOutboxC < Flezha::Tasklet
    tasklet_name "undo_outbox_c"
    def run(_input, _ctx) = raise("undo_outbox_c always fails")
  end

  class UndoOutboxWorkflow < Flezha::WorkflowOperative
    factory_id "undo_outbox_workflow"
    channels do |c|
      c.input("in", codec: :text).into(UndoOutboxA)
       .sink("mid", encode: ->(value) { "out:#{value}" })
       .into(UndoOutboxC)
    end
  end

  # -- branch_workflow ------------------------------------------------------
  #
  #     BranchA ─┬─▶ BranchB ─▶ BranchD
  #              └─▶ BranchC
  #
  # The tasklet TREE (workflow.rs:352). Every tasklet commits durable state
  # and compensates by emitting an undo record, so both tree behaviours are
  # observable at one sink:
  #
  #   * crash-resume — BranchD BLOCKS, so the SIGKILL lands with A, B and C
  #     committed and only D's subtree outstanding. The respawn re-runs D
  #     alone, seeded from B's journaled edge, so the sibling branch's `c:`
  #     marker appears exactly ONCE across both runs.
  #   * compensation — with FLEZHA_BRANCH_FAIL armed, BranchD fails instead of
  #     blocking. A, B and C have all completed, so the rollback unwinds the
  #     two branches before the root they share: `undo-c`, `undo-b`, `undo-a`.
  #
  # The inter-task channels are hybrid, which is what makes a non-leaf
  # tasklet's forward and undo output visible at the sink.

  BRANCH_FAIL_ENV = "FLEZHA_BRANCH_FAIL"

  class BranchA < UndoingStep
    tasklet_name "branch_a"
    def undo_record = "undo-a"
  end

  class BranchB < UndoingStep
    tasklet_name "branch_b"
    def undo_record = "undo-b"
  end

  # The sibling leaf: emits straight to the sink, and its undo does too.
  class BranchC < UndoingStep
    tasklet_name "branch_c"
    def undo_record = "undo-c"
    def released(input, _ctx) = "c:#{input}"
  end

  # The deep leaf under BranchB: blocks (crash-resume), or fails (compensation).
  class BranchD < Flezha::Tasklet
    tasklet_name "branch_d"

    def run(input, _ctx)
      raise "branch_d failed" if ENV.key?(BRANCH_FAIL_ENV)

      Fixtures.block_until_released
      "d:#{input}"
    end
  end

  class BranchWorkflow < Flezha::WorkflowOperative
    factory_id "branch_workflow"
    channels do |c|
      # One channel, two consumers: THE fan-out. Both branches read exactly
      # what BranchA released on it (held handle, wired twice).
      mid = c.input("in", codec: :text).into(BranchA)
             .sink("mid", encode: ->(value) { "a:#{value}" })
      # Fail fast into the one tasklet that fails: the compensation scenario
      # is about the rollback order, not the ladder that precedes it.
      mid.into(BranchB)
         .sink("bmid", encode: ->(value) { "b:#{value}" })
         .retry_max(1)
         .into(BranchD).sink("d_out")
      mid.into(BranchC).sink("c_out")
    end
  end

  # -- diamond_workflow -----------------------------------------------------
  #
  #     DiamA ─┬─▶ DiamB1 ─▶ DiamB2(blocks) ─┬─▶ DiamJoin
  #            └─▶ DiamC ────────────────────┘
  #
  # The fan-in DAG (workflow.rs:475). The join accumulates BOTH arms' outputs
  # and is dispatched exactly once, after both commit. DiamB2 blocks one level
  # BELOW its sibling arm — siblings commit at one level barrier — so a SIGKILL
  # lands with A, B1 and C committed and B2 mid-run: the respawn re-runs B2
  # alone, and the join runs once with C's JOURNALED handoff banked and B2's
  # fresh output appended, so `c:` appears exactly once across both runs.

  class DiamA < DurableStep
    tasklet_name "diam_a"
  end

  class DiamB1 < DurableStep
    tasklet_name "diam_b1"
  end

  # Waits BEFORE its change and its emit, so the kill window is deterministic:
  # B2 is always the uncommitted producer while the sibling arm has committed.
  class DiamB2 < DurableStep
    tasklet_name "diam_b2"
    def recorded(_input) = "diam_b"
    def gated?(_input) = true
    def released(input, _ctx) = "b:#{input}"
  end

  class DiamC < DurableStep
    tasklet_name "diam_c"
    def released(input, _ctx) = "c:#{input}"
  end

  # The join: one `j:`-tagged record per accumulated contribution.
  class DiamJoin < Flezha::Tasklet
    tasklet_name "diam_join"
    def run(input, _ctx) = "j:#{input}"
  end

  class DiamondWorkflow < Flezha::WorkflowOperative
    factory_id "diamond_workflow"
    channels do |c|
      # Held handle wired twice = the fan-out; two arms naming DiamJoin with no
      # node name = the fan-in, since two unnamed mentions are one node.
      fan = c.input("in", codec: :text).into(DiamA)
             .sink("dfan", encode: ->(value) { "a:#{value}" })
      fan.into(DiamB1).named("db1")
         .into(DiamB2).sink("db2")
         .into(DiamJoin)
      fan.into(DiamC).sink("dc")
         .into(DiamJoin).sink("j_out")
    end
  end

  # -- retry_workflow -------------------------------------------------------
  #
  # The per-cell retry ladder (workflow.rs:607). Task RA commits durable
  # state, forwards on a hybrid channel, and compensates by emitting an undo
  # record; `retry_flaky` fails its first FLEZHA_WORKFLOW_RETRY_FAILS attempts
  # (none by default — each scenario arms it) and then emits
  # `attempt:<n>:<input>`. One armed failure is absorbed by the ladder, so the
  # run completes and the sink sees attempt 2; arming more failures than the
  # ladder has attempts exhausts it, RA rolls back and the run reports an error.

  RETRY_FAILS_ENV = "FLEZHA_WORKFLOW_RETRY_FAILS"

  class RetryA < UndoingStep
    tasklet_name "retry_a"
  end

  class RetryFlaky < Flezha::Tasklet
    tasklet_name "retry_flaky"
    # Attempts made in this process. A retry re-runs the tasklet of the same
    # instance in place, so a process-wide counter is the whole ladder's view;
    # every worker thread shares this memory, so a mutex is enough.
    ATTEMPTS = { made: 0 }
    ATTEMPTS_LOCK = Mutex.new

    def run(input, _ctx)
      attempt = ATTEMPTS_LOCK.synchronize { ATTEMPTS[:made] += 1 }
      raise "flaky attempt #{attempt}" if attempt <= ENV.fetch(RETRY_FAILS_ENV, "").to_i

      "attempt:#{attempt}:#{input}"
    end
  end

  class RetryWorkflow < Flezha::WorkflowOperative
    factory_id "retry_workflow"
    channels do |c|
      c.input("in", codec: :text).into(RetryA)
       .sink("mid", encode: ->(value) { "res:#{value}" })
       .into(RetryFlaky).sink("out")
    end
  end

  # -- the durable loop family ----------------------------------------------
  #
  # chat_loop_workflow is the durable-loop + suspension shape
  # (workflow_loop.rs:8). One iteration per inbox item: TURN commits the turn
  # to Cell state, so a resumed iteration is visible in the WAL, and REPLY
  # emits the answer to the sink. Between turns the loop finds an empty inbox
  # at a committed boundary and PARKS — the instance is evicted and rehydrated
  # by the next message. The turn whose payload is the crash turn BLOCKS while
  # the release variable is unset, so a SIGKILL lands with the earlier
  # iterations durable and that one in flight. Keying the block on the payload
  # rather than on an invocation count keeps it deterministic across the
  # park/rehydrate cycles, which re-run the driver but not the turns. The turn
  # `bye` records nothing, still reaches REPLY, and completes the instance with
  # the number of turns TURN holds, so the session ends once that iteration
  # commits.
  #
  # stateless_loop_workflow is the same loop MINUS its stateful first
  # tasklet: ChatReply alone holds no per-Cell state, so nothing writes an
  # Cell WAL and the instance's task-journal reaches the driver only if it
  # rides the bind itself.
  #
  # chat_rewind_workflow is the loop whose replying tasklet can be undone.
  # The undo handle lives in Cell state, because a rewind's compensation is
  # rebuilt from the journal and is handed NO retained outputs, so the hook
  # reads the last thing it said, retracts it and pops it.

  CRASH_TURN = "mid-crash"
  BYE_TURN = "bye"

  class ChatTurn < DurableStep
    tasklet_name "chat_turn"
    # Keyed by the turn, so the record count is the number of DISTINCT turns
    # that ran; a re-run of a committed iteration would show up.
    def recorded(input) = input == BYE_TURN ? nil : input
    def gated?(input) = input == CRASH_TURN

    def released(input, ctx)
      ctx.complete(ctx.state[:items].size.to_s) if input == BYE_TURN
      input
    end
  end

  class ChatReply < Flezha::Tasklet
    tasklet_name "chat_reply"
    def run(input, _ctx) = "reply:#{input}"
  end

  class ChatReplyUndoable < Flezha::Tasklet
    tasklet_name "chat_reply_undoable"
    def initial_state(_cell_key) = { said: [] }

    def apply_change(state, change)
      change.key?(:said) ? state[:said].push(change[:said]) : state[:said].pop
    end

    def run(input, ctx)
      ctx.change({ said: input })
      "reply:#{input}"
    end

    def compensate(_outputs, ctx)
      turn = ctx.state[:said].last
      return if turn.nil?

      ctx.change({ retracted: true })
      ctx.emit("retract:#{turn}")
    end
  end

  # A loop here ends on an external stop, or on `bye` where ChatTurn runs; the
  # duration backstop keeps a stray instance from outliving the test run. The
  # knobs are declared once and inherited; `factory_id` deliberately is not.
  class LoopWorkflow < Flezha::WorkflowOperative
    loop_mode :per_input
    max_duration_ms 600_000
  end

  class ChatLoopWorkflow < LoopWorkflow
    factory_id "chat_loop_workflow"
    channels do |c|
      c.input("in", codec: :text).into(ChatTurn).named("mid")
       .into(ChatReply).sink("out")
    end
  end

  class StatelessLoopWorkflow < LoopWorkflow
    factory_id "stateless_loop_workflow"
    channels do |c|
      c.input("in", codec: :text).into(ChatReply)
       .sink("out")
    end
  end

  class ChatRewindWorkflow < LoopWorkflow
    factory_id "chat_rewind_workflow"
    channels do |c|
      c.input("in", codec: :text).into(ChatTurn).named("mid")
       .into(ChatReplyUndoable).sink("out")
    end
  end

  # The reply that blocks on the turn `reply-crash` while the release env is
  # unset, after the root has committed.
  REPLY_CRASH_TURN = "reply-crash"

  class BlockingReply < Flezha::Tasklet
    tasklet_name "blocking_reply"

    def run(input, _ctx)
      Fixtures.block_until_released if input == REPLY_CRASH_TURN
      "reply:#{input}"
    end
  end

  # The chat loop with the block moved AFTER the root's commit, so a SIGKILL
  # lands between a root's `task_committed` and the pass's `iter_committed`.
  class ReplyBlockLoopWorkflow < LoopWorkflow
    factory_id "reply_block_loop_workflow"
    channels do |c|
      c.input("in", codec: :text).into(ChatTurn).named("mid")
       .into(BlockingReply).sink("out")
    end
  end

  # -- the bench chains -----------------------------------------------------
  #
  # What `mix bench.workflow --scenario chain` drives (workflow_loop.rs:232): K
  # durable pass-through steps in a line, the payload unchanged, so the sink
  # receives the seed input and the bench reads its own seq and send timestamp
  # back. Each chain position needs its own tasklet, so the sixteen are
  # generated rather than written out; the edge names follow the reference,
  # which spells them from the step TYPE rather than from the tasklet name.

  BENCH_STEPS = (0..15).map do |at|
    Class.new(DurableStep) { tasklet_name "bench_s#{at}" }
  end.freeze

  def self.bench_chain(id, steps, persist)
    Class.new(Flezha::WorkflowOperative) do
      factory_id id
      persistent persist
      channels do |c|
        edge = c.input("in", codec: :text).into(BENCH_STEPS[0])
        (1...steps).each { |at| edge = edge.named("edge_BenchS#{at}").into(BENCH_STEPS[at]) }
        edge.sink("out")
      end
    end
  end

  BENCH_CHAINS = [
    bench_chain("bench_chain_1", 1, true),
    bench_chain("bench_chain_4", 4, true),
    bench_chain("bench_chain_16", 16, true),
    bench_chain("bench_chain_optout_4", 4, false)
  ].freeze

  # -- bench_loop_workflow --------------------------------------------------
  #
  # The long-lived loop the `turns` scenario drives (workflow_loop.rs:359): the
  # chat loop's shape with a BOUNDED per-turn state, so a million-turn soak
  # snapshots the same few bytes at every iteration, and a wake deadline for
  # the timer-wake measurements.

  WAKE_AFTER_ENV = "FLEZHA_BENCH_WAKE_AFTER_MS"

  class BenchTurn < Flezha::Tasklet
    tasklet_name "bench_turn"
    def initial_state(_cell_key) = { turns: 0, last: "" }

    def apply_change(state, change)
      state[:turns] += 1
      state[:last] = change[:turn]
    end

    def run(input, ctx)
      ctx.change({ turn: input })
      input
    end
  end

  class BenchLoopWorkflow < LoopWorkflow
    factory_id "bench_loop_workflow"
    # A day, not the chat loop's ten minutes: a soak outlives 600 s.
    max_duration_ms 24 * 3_600_000
    wake_after_ms ENV[WAKE_AFTER_ENV].to_i if ENV[WAKE_AFTER_ENV].to_i.positive?
    channels do |c|
      c.input("in", codec: :text).into(BenchTurn).named("mid")
       .into(ChatReply).sink("out")
    end
  end

  # -- workflow_timer_refused -----------------------------------------------
  #
  # A tasklet that registers a timer inside a workflow. Timers do not fire
  # there yet, so the ctx refuses naming `TimersUnavailable` and the instance
  # ends in error rather than losing the timer.

  class WorkflowTimer < Flezha::Tasklet
    tasklet_name "workflow_timer_refused"

    def run(_input, ctx)
      ctx.timer_after(60_000)
      nil
    end
  end

  class WorkflowTimerRefused < Flezha::WorkflowOperative
    factory_id "workflow_timer_refused"
    channels { |c| c.input("input", codec: :text).into(WorkflowTimer) }
  end

  # -- static_cell_workflow -------------------------------------------------
  #
  # A per-input loop whose one tasklet declares three static cells and routes
  # every message to two of them; each cell that runs emits `<cell key>:<message>`,
  # so one message yields exactly two outputs (workflow.rs).

  class StaticCellStep < Flezha::Tasklet
    tasklet_name "static_cell_step"
    cell_keys "a", "b", "c"

    def cells_for(_input) = %w[a b]

    def run(input, ctx)
      ctx.emit("#{ctx.cell_key}:#{input}")
      nil
    end
  end

  class StaticCellWorkflow < LoopWorkflow
    factory_id "static_cell_workflow"
    channels { |c| c.input("in", codec: :text).into(StaticCellStep).sink("out") }
  end

  # Every workflow operative this app registers, in the reference's order.
  WORKFLOWS = [
    EchoWorkflow, TypedOnceWorkflow, CommitBlockWorkflow, SyncDurableWorkflow, OptoutCommitBlock,
    TierCommitBlockWorkflow, SagaBlockWorkflow, KeyedBlockWorkflow, KeyedSagaWorkflow,
    OutboxBlockWorkflow, UndoOutboxWorkflow,
    ChatLoopWorkflow, StatelessLoopWorkflow, ChatRewindWorkflow, ReplyBlockLoopWorkflow,
    RetryWorkflow,
    BranchWorkflow, DiamondWorkflow, *BENCH_CHAINS, BenchLoopWorkflow, WorkflowTimerRefused,
    StaticCellWorkflow
  ].freeze
end
