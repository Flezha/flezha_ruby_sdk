# frozen_string_literal: true

require "etc"

require_relative "dispatch"
require_relative "logger"

module Flezha
  # The concurrency host: what runs `poll -> dispatch -> complete`, and on what.
  #
  # Two members in v1, and the seam between them may never observe or affect
  # per-cell ordering. Exclusivity is the core's handout guarantee — one
  # outstanding activation per Cell — and an activation is completed only
  # after its dispatch has returned, so ordering holds for any executor that
  # does not complete early. That invariant is this seam's entire contract.
  module Executor
    # What `pool_supervise` answers: spawn a base worker, stop, spawn a grown
    # one; anything else is nothing to do this wait.
    POOL_SPAWN_BASE = 1
    POOL_STOP = 2
    POOL_SPAWN_GROWN = 3

    module_function

    # One fetch loop, and the one place the three answers `poll` can give are
    # interpreted.
    #
    # Both executors fetch, and the deferred Ractor one will too — so what a
    # timeout, an activation and a shutdown each mean lives here rather than in
    # one correct copy per executor. The block is yielded each activation. The
    # loop ends when `stopping` answers true, when the core says it is shutting
    # down, and after `retire_ms` of timed-out polls when that is positive: the
    # governor's retire time, for a grown worker.
    def fetch(core, poll_ms, stopping, retire_ms = 0)
      idle = 0
      until stopping.call
        activation = core.poll(poll_ms)
        return :shutting_down if activation == :shutting_down

        if activation.nil?
          idle += poll_ms
          return :retired if retire_ms.positive? && idle >= retire_ms
        else
          idle = 0
          yield activation
        end
      end
      :stopped
    end

    # When a drain of `seconds` must be finished by.
    def deadline(seconds) = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds

    # How long is left of it, never negative — `Thread#join(0)` polls rather
    # than blocking, which is what a passed deadline should do.
    def remaining(deadline)
      [deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC), 0].max
    end

    # Say what the drain did not finish. Silence here is the failure mode the
    # bound exists to avoid reintroducing: a runner that leaves without
    # reporting what it abandoned looks identical to one that drained.
    def report_undrained(count, seconds)
      return if count.zero?

      Flezha.log.warn(
        "#{count} activation(s) were still running after the #{seconds}s drain; the " \
        "runner is exiting anyway. The core re-hands unanswered work on the next bind — find " \
        "the handler that does not return."
      )
    end

    # What both executors share: the core's pool governor sizes the pool over
    # this machine's processor count, and its supervisor's answers are what
    # this host spawns. A host only brings the threads.
    module Pooled
      attr_reader :base, :ceiling

      private

      def open_pool(core)
        @core = core
        @base, @ceiling, @poll_ms, @retire_ms, drain_ms = core.pool_open(Etc.nprocessors)
        @drain_s = drain_ms / 1000.0
      end

      # The base workers into the seats the governor reserved, then its supervisor.
      def start_pool
        @base.times { start_worker(false) }
        @supervisor = Thread.new { supervise }
        @supervisor.name = "flezha-supervisor"
      end

      # One governor answer per wait, at most a second apart, and the core's
      # notices written on each, so an idle runner's notices still reach its log.
      def supervise
        until @stopping
          answer = @core.pool_supervise
          Flezha.log_notices(@core.take_notices)
          break if answer == POOL_STOP || @stopping

          start_worker(answer == POOL_SPAWN_GROWN) if [POOL_SPAWN_BASE, POOL_SPAWN_GROWN].include?(answer)
        end
      rescue StandardError => e
        Flezha.log.error("the pool supervisor ended on #{e.class}: #{e.message}")
      end

      # A seat the governor reserved; a thread that cannot start gives it back.
      def start_worker(grown)
        spawn_member(grown)
      rescue ThreadError => e
        @core.pool_left
        Flezha.log.warn("could not start a worker thread: #{e.message}")
      end

      # A worker that dispatches on itself: the thread executor's every worker,
      # and a grown seat under either, because a parked completion needs a
      # thread and a stalled scheduler cannot supply one.
      def run_worker(grown)
        Executor.fetch(@core, @poll_ms, -> { @stopping }, grown ? @retire_ms : 0) do |activation|
          Dispatch.dispatch(activation, @core, @deps)
        end
      rescue StandardError => e
        # A worker that dies silently is a pool that shrinks with no report;
        # the governor replaces it, but only this line says why it left.
        Flezha.log.error("a worker thread ended on #{e.class}: #{e.message}")
      ensure
        @core.pool_left
      end
    end
  end

  # The default executor: a base pool of Ruby threads each looping
  # `poll -> dispatch -> complete` on itself, plus the core pool governor's
  # supervisor, whose answers grow the pool under starvation and restore it
  # when a worker left.
  #
  # This is the Rust host's model rather than Java's hand-off, and the reason
  # is the reason behind both shipped hosts: Java hands off because a parked
  # virtual thread costs a stack and nothing else — which is also exactly why
  # Java's fetchers never wait on user code. An MRI thread is a
  # real OS thread with a real stack, so hand-off would buy nothing and cost a
  # thread per activation up to the core's in-flight cap.
  #
  # Concurrency is bounded by the core's `FLEZHA_MAX_INFLIGHT_ACTIVATIONS`,
  # never by the pool size. The pool is a servicing detail.
  class ThreadExecutor
    include Executor::Pooled

    def initialize(core, deps)
      open_pool(core)
      @deps = deps
      @workers = []
      @supervisor = nil
      @lock = Mutex.new
      @stopping = false
    end

    def name = "threads"

    # How many workers are alive now. Reported in the startup line and read by
    # the pool-growth tests.
    def size = @lock.synchronize { @workers.count(&:alive?) }

    def start
      start_pool
      self
    end

    # Stop fetching without the core's word. A local run ends while the
    # process-wide poll core keeps running — there is no controller session to
    # shut down, and shutting the core would stop every later run in the
    # process — so the flag is raised first and the workers leave on their next
    # poll timeout instead of on the drain deadline.
    def stop_fetching
      @stopping = true
    end

    # Stop fetching and drain. The core says when: a worker leaves on the poll
    # that reports the core shutting down, which the core reports only once
    # the shutdown sequence has shipped every Cell's state through these
    # same workers; a worker inside a handler finishes it, completes it, and
    # only then observes it. Bounded by the governor's drain bound — a handler
    # that never returns must not keep the runner alive forever — and the flag
    # is raised after the join so a worker spawned late still leaves.
    def stop
      deadline = Executor.deadline(@drain_s)
      @lock.synchronize { @workers.dup }.each { |worker| worker.join(Executor.remaining(deadline)) }
      @stopping = true
      @supervisor&.join(Executor.remaining(deadline))
      undrained = @lock.synchronize { @workers.count(&:alive?) }
      Executor.report_undrained(undrained, @drain_s)
      @lock.synchronize { @workers.clear }
      Flezha.log_notices(@core.take_notices)
      undrained
    end

    private

    def spawn_member(grown)
      @lock.synchronize do
        @workers.select!(&:alive?)
        worker = Thread.new { run_worker(grown) }
        worker.name = "flezha-worker-#{@workers.size}"
        @workers << worker
      end
    end
  end

  # The opt-in executor for I/O-heavy handlers: real-thread fetchers hand
  # activations to fibers through a `Thread::Queue`, which is scheduler-aware,
  # so a handler's I/O yields instead of blocking a thread.
  #
  # The fetcher stays a real thread under this executor too. `poll` blocks in
  # native code, and `docs/internal/sdk/poll_core.md` is explicit that fetching on a
  # runtime's shared I/O pool starves everything else that pool serves.
  #
  # Completion happens on the fiber that dispatched, not on the fetcher: the
  # ABI accepts a completion from any thread, and the ordering guarantee is the
  # core's handout, not this executor's.
  class FiberExecutor
    include Executor::Pooled

    # The queue depth between the fetchers and the fibers. Bounded so a fetcher
    # cannot run arbitrarily far ahead of the handlers; the core's in-flight
    # cap bounds the total either way.
    QUEUE_DEPTH = 64

    def initialize(core, deps, scheduler: nil)
      open_pool(core)
      @deps = deps
      @scheduler = scheduler
      @queue = Thread::SizedQueue.new(QUEUE_DEPTH)
      @threads = []
      @stopping = false
    end

    def name = "fibers"
    def size = @threads.count(&:alive?)

    def start
      @dispatcher = Thread.new { run_dispatcher }
      @dispatcher.name = "flezha-fibers"
      start_pool
      self
    end

    # Stop fetching without the core's word; see the thread executor's.
    def stop_fetching
      @stopping = true
    end

    # The fetchers leave on the core's word, as the thread executor's workers
    # do; the flag is raised after their join.
    def stop
      deadline = Executor.deadline(@drain_s)
      @threads.dup.each { |thread| thread.join(Executor.remaining(deadline)) }
      @stopping = true
      @supervisor&.join(Executor.remaining(deadline))
      # Closing the queue is what ends the dispatcher: a scheduler-aware pop
      # answers nil on a closed queue rather than parking forever.
      @queue.close
      # Bounded for the reason the thread executor's join is: closing a
      # scheduler runs its outstanding fibers to completion, and a fiber that
      # never returns would hold the drain open forever.
      @dispatcher&.join(Executor.remaining(deadline))
      undrained = [@dispatcher, *@threads].count { |thread| thread&.alive? }
      Executor.report_undrained(undrained, @drain_s)
      Flezha.log_notices(@core.take_notices)
      undrained
    end

    private

    # A fetcher for a base seat; a grown seat is a worker that dispatches on
    # itself, since a scheduler stalled in a parked completion cannot rescue itself.
    def spawn_member(grown)
      @threads.select!(&:alive?)
      thread = Thread.new { grown ? run_worker(true) : run_fetcher }
      thread.name = "flezha-fetcher-#{@threads.size}"
      @threads << thread
    end

    def run_fetcher
      Executor.fetch(@core, @poll_ms, -> { @stopping }) { |activation| @queue.push(activation) }
    rescue ClosedQueueError
      nil
    rescue StandardError => e
      Flezha.log.error("a fetcher thread ended on #{e.class}: #{e.message}")
    ensure
      @core.pool_left
    end

    # The pop runs in a scheduled fiber, so a quiet queue yields to the event
    # loop and a handler fiber is resumed as soon as its I/O is ready. That loop
    # is the scheduler's close, which Ruby runs when this thread ends and which
    # runs every fiber to completion, the pop's included: a drain here means
    # what it means under the thread executor. Closing the scheduler by hand as
    # well would be a second close, which the scheduler refuses.
    def run_dispatcher
      Fiber.set_scheduler(build_scheduler)
      Fiber.schedule { pop_and_dispatch }
    rescue StandardError => e
      Flezha.log.error("the fiber dispatcher ended on #{e.class}: #{e.message}")
    end

    def pop_and_dispatch
      while (activation = @queue.pop)
        Fiber.schedule { Dispatch.dispatch(activation, @core, @deps) }
      end
    rescue StandardError => e
      Flezha.log.error("the fiber dispatcher ended on #{e.class}: #{e.message}")
    end

    def build_scheduler
      return @scheduler.call if @scheduler

      begin
        require "async"
      rescue LoadError
        raise Error.new("runtime.executor_unavailable",
                        executor: "fibers",
                        reason: "it needs a Fiber::Scheduler; add the `async` gem, or pass `scheduler:` " \
                                "a callable answering one")
      end
      Async::Scheduler.new
    end
  end
end
