#include <thread>

#include <gtest/gtest.h>

#include <Common/CurrentThread.h>
#include <Common/FailPoint.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/setThreadName.h>
#include <Common/tests/gtest_global_context.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace FailPoints
{
    extern const char thread_group_switcher_attach_failure[];
    extern const char thread_group_link_thread_failure[];
}

/// Regression test for https://github.com/ClickHouse/clickhouse-core-incidents/issues/1682
///
/// Root cause (confirmed from stack traces):
///   TasksStatsCounters::reset() inside initPerformanceCounters() doesn't have
///   try/catch. When /proc/thread-self/schedstat returned errno=9 (EBADF), the
///   ErrnoException propagated to the ThreadGroupSwitcher constructor's noexcept catch
///   block. That block cleared switcher.thread_group but left ThreadStatus::thread_group
///   set to the group that was assigned earlier in attachToGroupImpl(). The pool worker
///   thread stayed attached to the stale group, so every subsequent ThreadGroupSwitcher
///   on that thread threw LOGICAL_ERROR:
///     "Thread (REMOTE_FS_READ_THREAD_POOL) is already attached to a group (master_thread_id 1398)"
///
/// The failpoint thread_group_switcher_attach_failure fires inside attachToGroupImpl()
/// right after `thread_group = thread_group_`, reproducing the exact failure window.
///
/// Fix: the constructor catch block now checks whether ThreadStatus is partially
/// attached to our group and calls detachFromGroupIfNotDetached() to undo it.
///
/// NOTE: ThreadGroupSwitcher::ThreadGroupSwitcher is noexcept. It has an internal
/// catch-all that swallows all exceptions (including the injected failpoint one) and
/// logs them.
TEST(ThreadGroupSwitcher, PartialAttachUndoneOnException)
{
    auto context = getContext().context;

    std::exception_ptr ex;
    bool second_switcher_succeeded = false;

    std::thread t([&]
    {
        try
        {
            ThreadStatus ts;

            auto G1 = std::make_shared<ThreadGroup>(context, 0);
            auto G2 = std::make_shared<ThreadGroup>(context, 0);

            /// Enable the failpoint: the next attachToGroupImpl() call will throw after
            /// setting ThreadStatus::thread_group, simulating the failure path.
            FailPointInjection::enableFailPoint(FailPoints::thread_group_switcher_attach_failure);

            {
                /// The constructor catches the injected exception internally (noexcept).
                /// Before the fix: ThreadStatus::thread_group stays set to G1 → stale.
                /// After  the fix: catch block detects partial attachment and undoes it.
                ThreadGroupSwitcher switcher(G1, ThreadName::REMOTE_FS_READ_THREAD_POOL);

                /// The constructor never throws — the exception was swallowed above.
                /// With the fix, the thread is already detached here.
            }

            /// Failpoint is ONCE — already consumed, no need to disable.

            /// With the fix the thread is clean: the second attachment must succeed.
            /// Without the fix the stale G1 is still set and this constructor throws
            /// LOGICAL_ERROR "already attached" (caught internally, switcher is a no-op).
            {
                ThreadGroupSwitcher switcher(G2, ThreadName::REMOTE_FS_READ_THREAD_POOL);
                second_switcher_succeeded = (getCurrentThreadGroup() == G2);
            }

            ASSERT_EQ(getCurrentThreadGroup(), nullptr);
        }
        catch (...)
        {
            ex = std::current_exception();
        }
    });
    t.join();

    if (ex)
        std::rethrow_exception(ex);

    EXPECT_TRUE(second_switcher_succeeded)
        << "Second ThreadGroupSwitcher must attach successfully after the first one "
           "cleaned up its partial attachment; without the fix the stale group from the "
           "failed first attachment would block every subsequent task on this pool worker";
}

/// When linkThread() itself throws (e.g. bad_alloc from thread_ids.insert), active_thread_count
/// was never incremented. The SCOPE_EXIT_SAFE rollback in attachToGroupImpl must NOT call
/// unlinkThread() in that case, or it would corrupt the counter and hit the chassert.
/// After the failure the thread must be clean and able to accept the next attachment.
TEST(ThreadGroupSwitcher, LinkThreadFailureDoesNotCorruptCounter)
{
    auto context = getContext().context;

    std::exception_ptr ex;
    bool second_switcher_succeeded = false;

    std::thread t([&]
    {
        try
        {
            ThreadStatus ts;

            auto G1 = std::make_shared<ThreadGroup>(context, 0);
            auto G2 = std::make_shared<ThreadGroup>(context, 0);

            FailPointInjection::enableFailPoint(FailPoints::thread_group_link_thread_failure);

            {
                /// linkThread throws before active_thread_count is incremented.
                /// The SCOPE_EXIT rollback must skip unlinkThread (linked=false).
                /// ThreadStatus::thread_group is never set, so the thread is immediately clean.
                ThreadGroupSwitcher switcher(G1, ThreadName::REMOTE_FS_READ_THREAD_POOL);
            }

            /// Thread is clean — second attachment must succeed.
            {
                ThreadGroupSwitcher switcher(G2, ThreadName::REMOTE_FS_READ_THREAD_POOL);
                second_switcher_succeeded = (getCurrentThreadGroup() == G2);
            }

            ASSERT_EQ(getCurrentThreadGroup(), nullptr);
        }
        catch (...)
        {
            ex = std::current_exception();
        }
    });
    t.join();

    if (ex)
        std::rethrow_exception(ex);

    EXPECT_TRUE(second_switcher_succeeded)
        << "linkThread failure must not corrupt active_thread_count and must leave the "
           "thread clean for the next attachment";
}

} // namespace DB
