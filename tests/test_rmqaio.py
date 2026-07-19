import asyncio
import gc
import itertools

from dataclasses import FrozenInstanceError
from ssl import PROTOCOL_TLS_CLIENT, SSLContext
from unittest import mock

import aiormq
import pytest

from rmqaio import (
    Config,
    Connection,
    ConnectionState,
    Ops,
    Repeat,
    RetryPolicy,
    SharedConnection,
)
from rmqaio.rmqaio import (
    ConnectionInvalidStateError,
    _as_int_or_none,
    _env_var_as_bool,
    _env_var_as_int,
    retry,
)


class TestConfig:
    def test_default_values(self, monkeypatch):
        monkeypatch.delenv("RMQAIO_LOG_SANITIZE", raising=False)
        config = Config()
        assert config.log_sanitize is True

    def test_log_sanitize_from_env_true(self, monkeypatch):
        monkeypatch.setenv("RMQAIO_LOG_SANITIZE", "true")
        config = Config()
        assert config.log_sanitize is True

    def test_log_sanitize_from_env_false(self, monkeypatch):
        monkeypatch.setenv("RMQAIO_LOG_SANITIZE", "false")
        config = Config()
        assert config.log_sanitize is False

    def test_log_sanitize_from_env_invalid(self, monkeypatch):
        monkeypatch.setenv("RMQAIO_LOG_SANITIZE", "abc")
        config = Config()
        assert config.log_sanitize is True

    def test_log_data_truncate_size_default(self, monkeypatch):
        monkeypatch.delenv("RMQAIO_LOG_DATA_TRUNCATE_SIZE", raising=False)
        config = Config()
        assert config.log_data_truncate_size == 10000

    def test_log_data_truncate_size_from_env(self, monkeypatch):
        monkeypatch.setenv("RMQAIO_LOG_DATA_TRUNCATE_SIZE", "1024")
        config = Config()
        assert config.log_data_truncate_size == 1024

    def test_log_data_truncate_size_from_env_invalid(self, monkeypatch):
        monkeypatch.setenv("RMQAIO_LOG_DATA_TRUNCATE_SIZE", "abc")
        with pytest.raises(ValueError):
            Config()


class TestRetryPolicy:
    def test_default_values(self):
        policy = RetryPolicy()
        assert policy.delays == Repeat(5)
        assert isinstance(policy.exc_filter, tuple)
        assert policy.exc_filter == (
            asyncio.TimeoutError,
            ConnectionError,
            aiormq.exceptions.AMQPConnectionError,
        )

    def test_custom_delays(self):
        policy = RetryPolicy(delays=[1, 2, 3])
        assert policy.delays == [1, 2, 3]

    def test_is_retryable_with_tuple(self):
        policy = RetryPolicy(exc_filter=(ValueError, TypeError))
        assert policy.is_retryable(ValueError()) is True
        assert policy.is_retryable(TypeError()) is True
        assert policy.is_retryable(RuntimeError()) is False

    def test_is_retryable_with_callable(self):
        policy = RetryPolicy(exc_filter=lambda e: isinstance(e, (ValueError, TypeError)))
        assert policy.is_retryable(ValueError()) is True
        assert policy.is_retryable(TypeError()) is True
        assert policy.is_retryable(RuntimeError()) is False

    def test_hash(self):
        policy1 = RetryPolicy(delays=[1, 2, 3], exc_filter=(ValueError,))
        policy2 = RetryPolicy(delays=[1, 2, 3], exc_filter=(ValueError,))
        policy3 = RetryPolicy(delays=[1, 2, 4], exc_filter=(ValueError,))

        assert hash(policy1) == hash(policy2)
        assert hash(policy1) != hash(policy3)

    def test_retry_policy_immutable(self):
        policy = RetryPolicy()
        with pytest.raises(FrozenInstanceError):
            policy.delays = [1, 2]


class TestRepeat:
    def test_init_with_int(self):
        r = Repeat(5)
        assert r.value == 5
        assert list(itertools.islice(r, 3)) == [5, 5, 5]

    def test_init_with_float(self):
        r = Repeat(2.5)
        assert r.value == 2.5
        assert list(itertools.islice(r, 2)) == [2.5, 2.5]

    def test_hash_same_value(self):
        r1 = Repeat(5)
        r2 = Repeat(5)
        assert hash(r1) == hash(r2)

    def test_hash_different_values(self):
        r1 = Repeat(5)
        r2 = Repeat(10)
        assert hash(r1) != hash(r2)

    def test_hash_int_and_float_equivalent(self):
        r1 = Repeat(5)
        r2 = Repeat(5.0)
        assert hash(r1) == hash(r2)

    def test_eq_same_value(self):
        r1 = Repeat(5)
        r2 = Repeat(5)
        assert r1 == r2

    def test_eq_different_value(self):
        r1 = Repeat(5)
        r2 = Repeat(10)
        assert r1 != r2

    def test_eq_with_non_Repeat(self):
        r = Repeat(5)
        assert (r == 5) is False
        assert (r == "test") is False

    def test_ne_same_value(self):
        r1 = Repeat(5)
        r2 = Repeat(5)
        assert (r1 != r2) is False

    def test_ne_different_value(self):
        r1 = Repeat(5)
        r2 = Repeat(10)
        assert r1 != r2

    def test_ne_with_non_Repeat(self):
        r = Repeat(5)
        assert r != 5
        assert r != "test"


class TestRetry:
    @pytest.mark.asyncio
    async def test_success_no_retry(self):
        call_count = 0

        @retry(RetryPolicy(delays=[0]))
        async def fn():
            nonlocal call_count
            call_count += 1
            return "ok"

        result = await fn()
        assert result == "ok"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_retry_then_success(self):
        call_count = 0

        @retry(RetryPolicy(delays=[0, 0]))
        async def fn():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("first fail")
            return "ok"

        result = await fn()
        assert result == "ok"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_retry_exhausted(self):
        call_count = 0

        @retry(RetryPolicy(delays=[0]))
        async def fn():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("always fail")

        with pytest.raises(ConnectionError, match="always fail"):
            await fn()
        assert call_count == 2  # initial + 1 retry

    @pytest.mark.asyncio
    async def test_non_retryable_exception(self):
        @retry(RetryPolicy(delays=[0]))
        async def fn():
            raise ValueError("not retryable")

        with pytest.raises(ValueError, match="not retryable"):
            await fn()

    @pytest.mark.asyncio
    async def test_on_error_callback(self):
        on_error_called = asyncio.Event()

        async def on_error(e):
            on_error_called.set()

        call_count = 0

        @retry(RetryPolicy(delays=[0]), on_error=on_error)
        async def fn():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("fail")
            return "ok"

        result = await fn()
        assert result == "ok"
        assert on_error_called.is_set()

    @pytest.mark.asyncio
    async def test_custom_msg(self):
        @retry(RetryPolicy(delays=[0]), msg="custom message")
        async def fn():
            raise ConnectionError("fail")

        with pytest.raises(ConnectionError):
            await fn()


class TestConnection:
    def test_init(self):
        conn = Connection("amqp://admin@example.com")
        assert conn._conn is None
        assert conn._channel is None
        assert conn._state == ConnectionState.INITIAL
        assert conn._id is not None
        assert conn._url == "amqp://admin@example.com"
        assert conn._ssl_context is None
        assert conn._open_retry_policy is None
        assert conn._reopen_retry_policy is not None
        assert conn._loop_task is None
        assert conn._exc is None
        assert conn._callbacks == {}

    def test_init_with_ssl_context(self):
        ssl_ctx = SSLContext(protocol=PROTOCOL_TLS_CLIENT)
        conn = Connection("amqp://admin@example.com", ssl_context=ssl_ctx)
        assert conn._ssl_context is ssl_ctx

    def test_init_with_retry_policy(self):
        policy = RetryPolicy(delays=[1, 2, 3])

        conn = Connection("amqp://admin@example.com", open_retry_policy=policy)
        assert conn._open_retry_policy is policy
        assert conn._reopen_retry_policy is not None

        conn = Connection("amqp://admin@example.com", reopen_retry_policy=policy)
        assert conn._open_retry_policy is None
        assert conn._reopen_retry_policy is policy

    def test_connection_timeout_from_url(self):
        conn = Connection("amqp://admin@example.com?connection_timeout=30000")
        assert conn._connect_timeout == 30

    def test_connection_timeout_not_set(self):
        conn = Connection("amqp://admin@example.com")
        assert conn._connect_timeout is None

    def test_properties(self):
        conn = Connection("amqp://admin@example.com")
        assert conn.id is not None
        assert conn.url == "amqp://admin@example.com"
        assert conn.ssl_context is None
        assert conn.open_retry_policy is None
        assert conn.reopen_retry_policy is not None
        assert conn.is_open is False
        assert conn.is_closed is False

    def test_str(self):
        conn = Connection("amqp://admin@example.com")
        assert "Connection[example.com]" in str(conn)

    def test_str_with_port(self):
        conn = Connection("amqp://admin@example.com:5672")
        assert "Connection[example.com:5672]" in str(conn)

    def test_repr(self):
        conn = Connection("amqp://admin@example.com")
        assert "Connection[example.com]" in repr(conn)

    @pytest.mark.asyncio
    async def test_open(self, mock_aiormq):
        conn = Connection("amqp://admin@example.com")
        await conn.open()
        mock_aiormq.assert_awaited_once_with("amqp://admin@example.com", context=None)
        assert conn.is_open is True
        assert conn.is_closed is False
        await conn.close()

    @pytest.mark.asyncio
    async def test_open_already_open(self, mock_aiormq):
        conn = Connection("amqp://admin@example.com")
        await conn.open()
        await conn.open()
        mock_aiormq.assert_awaited_once()
        await conn.close()

    @pytest.mark.asyncio
    async def test_open_connection_failure(self):
        with mock.patch("aiormq.connect", new_callable=mock.AsyncMock) as mock_connect:
            mock_connect.side_effect = Exception("Connection failed")
            conn = Connection("amqp://admin@example.com")
            with pytest.raises(Exception, match="Connection failed"):
                await conn.open()

    @pytest.mark.asyncio
    async def test_open_with_ssl_context(self, mock_aiormq):
        ssl_ctx = SSLContext(protocol=PROTOCOL_TLS_CLIENT)

        conn = Connection("amqp://admin@example.com", ssl_context=ssl_ctx)
        await conn.open()

        mock_aiormq.assert_awaited_once()
        call_kwargs = mock_aiormq.call_args.kwargs
        assert call_kwargs.get("context") is ssl_ctx

        await conn.close()

    @pytest.mark.asyncio
    async def test_close(self, mock_aiormq):
        conn = Connection("amqp://admin@example.com")
        await conn.open()
        await conn.close()
        assert conn.is_open is False
        assert conn.is_closed is True
        assert conn._conn is None
        assert conn._channel is None
        assert conn._loop_task is None

    @pytest.mark.asyncio
    async def test_close_already_closed(self, mock_aiormq):
        conn = Connection("amqp://admin@example.com")
        await conn.close()
        await conn.close()

    @pytest.mark.asyncio
    async def test_close_from_inside_loop_task_does_not_cancel_loop_task(self, mock_aiormq):
        conn = Connection("amqp://admin@example.com")
        await conn.open()
        assert conn._loop_task is not None

        loop_task = conn._loop_task

        async def close_from_another_task():
            await conn.close()

        await close_from_another_task()

        assert conn.is_closed is True
        assert conn._conn is None
        assert conn._loop_task is None
        assert loop_task.done()

    @pytest.mark.asyncio
    async def test_new_channel(self, mock_aiormq):
        conn = Connection("amqp://admin@example.com")
        channel = await conn.new_channel()
        assert channel is not None
        assert conn._channel is None
        assert await conn.new_channel() != await conn.new_channel()
        await conn.close()

    @pytest.mark.asyncio
    async def test_channel(self, mock_aiormq):
        conn = Connection("amqp://admin@example.com")
        channel = await conn.channel()
        assert channel is not None
        assert conn._channel is not None
        assert await conn.channel() == await conn.channel()
        await conn.close()

    @pytest.mark.asyncio
    async def test_new_channel_with_timeout(self, mock_aiormq):
        conn = Connection("amqp://admin@example.com")
        with mock.patch.object(conn, "open", wraps=conn.open) as open_mock:
            await conn.new_channel(timeout=42)
            open_mock.assert_awaited_once_with(timeout=42)
        await conn.close()

    @pytest.mark.asyncio
    async def test_channel_with_timeout(self, mock_aiormq):
        conn = Connection("amqp://admin@example.com")
        with mock.patch.object(conn, "open", wraps=conn.open) as open_mock:
            await conn.channel(timeout=42)
            open_mock.assert_awaited_once_with(timeout=42)
        await conn.close()

    @pytest.mark.asyncio
    async def test_callbacks(self):
        callback_invoked = asyncio.Event()

        async def callback(state_from, state_to):
            callback_invoked.set()

        conn = Connection("amqp://admin@example.com")

        conn.set_callback("test", callback)
        assert conn._callbacks["test"] == callback

        await conn._execute_callbacks(ConnectionState.INITIAL, ConnectionState.CONNECTING)
        assert callback_invoked.is_set()

        await conn.remove_callback("test")
        assert "test" not in conn._callbacks

    @pytest.mark.asyncio
    async def test_bound_method_callback_does_not_leak(self):
        conn = Connection("amqp://admin@example.com")
        ops = Ops(conn)

        callback_name = f"on_connection_state_changed[{id(ops)}]"
        assert callback_name in conn._callbacks

        del ops
        gc.collect()

        await conn._execute_callbacks(ConnectionState.RECONNECTING, ConnectionState.CONNECTED)

        assert callback_name not in conn._callbacks


class TestCallbackSystem:
    """End-to-end verification of the connection-state callback system.

    These tests assert the *full sequence* of (from -> to) transitions
    delivered to registered callbacks for every lifecycle path: open,
    refresh, graceful close, and connection loss / automatic reconnect.
    """

    @pytest.mark.asyncio
    async def test_callbacks_on_open_sequence(self, mock_aiormq):
        transitions: list[tuple[ConnectionState, ConnectionState]] = []

        async def callback(state_from, state_to):
            transitions.append((state_from, state_to))

        conn = Connection("amqp://admin@example.com")
        conn.set_callback("recorder", callback)

        await conn.open()

        assert (ConnectionState.INITIAL, ConnectionState.CONNECTING) in transitions
        assert (ConnectionState.CONNECTING, ConnectionState.CONNECTED) in transitions
        assert conn.is_open

        await conn.close()

    @pytest.mark.asyncio
    async def test_callbacks_on_refresh_sequence(self, mock_aiormq):
        transitions: list[tuple[ConnectionState, ConnectionState]] = []

        async def callback(state_from, state_to):
            transitions.append((state_from, state_to))

        conn = Connection("amqp://admin@example.com")
        conn.set_callback("recorder", callback)

        await conn.open()
        transitions.clear()

        await conn.refresh()

        assert (ConnectionState.CONNECTED, ConnectionState.REFRESHING) in transitions
        assert (ConnectionState.REFRESHING, ConnectionState.CONNECTED) in transitions
        assert conn.is_open

        await conn.close()

    @pytest.mark.asyncio
    async def test_callbacks_on_close_sequence(self, mock_aiormq):
        transitions: list[tuple[ConnectionState, ConnectionState]] = []

        async def callback(state_from, state_to):
            transitions.append((state_from, state_to))

        conn = Connection("amqp://admin@example.com")
        conn.set_callback("recorder", callback)

        await conn.open()
        transitions.clear()

        await conn.close()

        assert (ConnectionState.CONNECTED, ConnectionState.CLOSING) in transitions
        assert (ConnectionState.CLOSING, ConnectionState.CLOSED) in transitions
        assert conn.is_closed

    @pytest.mark.asyncio
    async def test_callbacks_on_connection_lost_reconnect(self, mock_aiormq):
        transitions: list[tuple[ConnectionState, ConnectionState]] = []

        async def callback(state_from, state_to):
            transitions.append((state_from, state_to))

        conn = Connection("amqp://admin@example.com")
        conn.set_callback("recorder", callback)

        await conn.open()

        # Simulate broker connection loss: resolve the live connection's closing future.
        live_conn = conn._conn
        assert live_conn is not None
        transitions.clear()
        live_conn.closing.set_result(None)

        # Wait for the automatic reconnect to bring the connection back up.
        for _ in range(50):
            if conn.is_open and (ConnectionState.RECONNECTING, ConnectionState.CONNECTED) in transitions:
                break
            await asyncio.sleep(0.05)

        assert (ConnectionState.CONNECTED, ConnectionState.RECONNECTING) in transitions
        assert (ConnectionState.RECONNECTING, ConnectionState.CONNECTED) in transitions
        assert conn.is_open

        await conn.close()

    @pytest.mark.asyncio
    async def test_callbacks_fire_on_open_failure(self):
        transitions: list[tuple[ConnectionState, ConnectionState]] = []

        async def callback(state_from, state_to):
            transitions.append((state_from, state_to))

        with mock.patch("aiormq.connect", new_callable=mock.AsyncMock, side_effect=Exception("boom")):
            conn = Connection("amqp://admin@example.com", open_retry_policy=RetryPolicy(delays=[]))
            conn.set_callback("recorder", callback)

            with pytest.raises(Exception):
                await conn.open()

        assert (ConnectionState.INITIAL, ConnectionState.CONNECTING) in transitions
        assert conn._state == ConnectionState.INITIAL

    @pytest.mark.asyncio
    async def test_multiple_callbacks_all_invoked(self, mock_aiormq):
        calls: dict[str, int] = {}

        async def make_callback(name):
            async def cb(state_from, state_to):
                calls[name] = calls.get(name, 0) + 1

            return cb

        conn = Connection("amqp://admin@example.com")
        conn.set_callback("a", await make_callback("a"))
        conn.set_callback("b", await make_callback("b"))

        await conn.open()
        await conn.close()

        assert calls["a"] >= 2
        assert calls["b"] >= 2
        assert calls["a"] == calls["b"]

    @pytest.mark.asyncio
    async def test_remove_callback_stops_invocation(self, mock_aiormq):
        calls = 0

        async def callback(state_from, state_to):
            nonlocal calls
            calls += 1

        conn = Connection("amqp://admin@example.com")
        conn.set_callback("test", callback)

        await conn.open()
        await conn.remove_callback("test")

        # Trigger another transition (close) - callback must NOT be called anymore.
        calls_before = calls
        await conn.close()

        assert calls == calls_before


class TestConcurrency:
    """Verify open/refresh/close behave correctly under concurrent calls.

    No locks are used; correctness relies on asyncio being single-threaded
    (synchronous sections are atomic) and on state/future guarding.
    """

    @pytest.mark.asyncio
    async def test_concurrent_open_uses_single_loop_task(self, mock_aiormq):
        conn = Connection("amqp://admin@example.com")

        await asyncio.gather(conn.open(), conn.open())

        assert conn.is_open
        assert conn._loop_task is not None
        assert conn._open_future is None or conn._open_future.done()
        await conn.close()
        assert conn.is_closed
        assert conn._loop_task is None

    @pytest.mark.asyncio
    async def test_concurrent_refresh_waits_for_first(self, mock_aiormq):
        conn = Connection("amqp://admin@example.com")
        await conn.open()

        await asyncio.gather(conn.refresh(), conn.refresh())

        assert conn.is_open
        # Exactly one reconnect happened: loop task still single, future cleared.
        assert conn._loop_task is not None
        await conn.close()
        assert conn.is_closed

    @pytest.mark.asyncio
    async def test_concurrent_open_and_close(self, mock_aiormq):
        conn = Connection("amqp://admin@example.com")

        # close() may win the race; open() then fails with "connection closed".
        # Either way the connection must end up fully closed without leaking tasks.
        results = await asyncio.gather(conn.open(), conn.close(), return_exceptions=True)

        assert conn.is_closed
        assert conn._loop_task is None
        assert conn._conn is None
        # Exactly one of the two raised (or neither): no unexpected exceptions.
        assert all(r is None or isinstance(r, ConnectionInvalidStateError) for r in results)

    @pytest.mark.asyncio
    async def test_concurrent_refresh_and_close(self, mock_aiormq):
        conn = Connection("amqp://admin@example.com")
        await conn.open()

        results = await asyncio.gather(conn.refresh(), conn.close(), return_exceptions=True)

        assert conn.is_closed
        assert conn._loop_task is None
        assert conn._conn is None
        assert all(r is None or isinstance(r, ConnectionInvalidStateError) for r in results)

    @pytest.mark.asyncio
    async def test_concurrent_close_idempotent(self, mock_aiormq):
        conn = Connection("amqp://admin@example.com")
        await conn.open()

        await asyncio.gather(conn.close(), conn.close(), conn.close())

        assert conn.is_closed
        assert conn._loop_task is None
        assert conn._conn is None

    @pytest.mark.asyncio
    async def test_open_during_refresh_waits_for_completion(self, mock_aiormq):
        conn = Connection("amqp://admin@example.com")
        await conn.open()

        await asyncio.gather(conn.refresh(), conn.open())

        assert conn.is_open
        await conn.close()
        assert conn.is_closed


class TestSharedConnection:
    @pytest.mark.asyncio
    async def test_init(self):
        conn = SharedConnection("amqp://admin@example.com")
        assert conn._key is not None
        assert conn._lock is not None
        assert conn._conn is not None
        assert conn._shared_item is not None
        assert conn._channel is None
        assert conn._is_open is not None
        assert conn._is_closed is not None

    @pytest.mark.asyncio
    async def test_properties(self):
        conn = SharedConnection("amqp://admin@example.com")
        assert conn.url == "amqp://admin@example.com"
        assert conn.ssl_context is None
        assert conn.open_retry_policy is None
        assert conn.reopen_retry_policy is not None
        assert conn.is_open is False
        assert conn.is_closed is False

    @pytest.mark.asyncio
    async def test_init_with_name(self):
        conn = SharedConnection("amqp://admin@rabbitmq.com")
        assert str(conn.url) == "amqp://admin@rabbitmq.com"

    @pytest.mark.asyncio
    async def test_init_with_ssl_context(self):
        ssl_ctx = SSLContext(protocol=PROTOCOL_TLS_CLIENT)
        conn = SharedConnection("amqp://admin@example.com", ssl_context=ssl_ctx)
        assert conn.ssl_context is ssl_ctx

    @pytest.mark.asyncio
    async def test_init_with_retry_policy(self):
        policy = RetryPolicy(delays=[1, 2, 3])

        conn = SharedConnection("amqp://admin@example.com", open_retry_policy=policy)
        assert conn.open_retry_policy is policy

        conn = SharedConnection("amqp://admin@example.com", reopen_retry_policy=policy)
        assert conn.reopen_retry_policy is policy

    @pytest.mark.asyncio
    async def test_str(self):
        conn = SharedConnection("amqp://admin@example.com")
        result = str(conn)
        assert result.startswith("SharedConnection[Connection[example.com]")

    @pytest.mark.asyncio
    async def test_str_with_port(self):
        conn = SharedConnection("amqp://admin@example.com:5672")
        result = str(conn)
        assert result.startswith("SharedConnection[Connection[example.com:5672]")

    @pytest.mark.asyncio
    async def test_repr(self):
        conn = SharedConnection("amqp://admin@example.com")
        result = repr(conn)
        assert result.startswith("SharedConnection[Connection[example.com]")

    @pytest.mark.asyncio
    async def test_open_close(self, mock_aiormq):
        conn = SharedConnection("amqp://admin@example.com")
        await conn.open()
        mock_aiormq.assert_awaited_once_with("amqp://admin@example.com", context=None)
        assert conn.is_open is True
        assert conn.is_closed is False
        await conn.close()
        assert conn.is_open is False
        assert conn.is_closed is True

    @pytest.mark.asyncio
    async def test_open_already_open(self, mock_aiormq):
        conn = SharedConnection("amqp://admin@example.com")
        await conn.open()
        await conn.open()
        mock_aiormq.assert_awaited_once()
        await conn.close()

    @pytest.mark.asyncio
    async def test_shared_same_connection(self, mock_aiormq):
        conn1 = SharedConnection(
            "amqp://admin@example.com",
            open_retry_policy=RetryPolicy(delays=[0]),
            reopen_retry_policy=RetryPolicy(delays=[1, 2]),
        )
        conn2 = SharedConnection(
            "amqp://admin@example.com",
            open_retry_policy=RetryPolicy(delays=[0]),
            reopen_retry_policy=RetryPolicy(delays=[1, 2]),
        )

        assert conn1._conn == conn2._conn

        await conn1.open()

        assert conn1.is_open is True
        assert conn2.is_open is False

        await conn1.close()
        await conn2.close()

    @pytest.mark.asyncio
    async def test_shared_not_same_connection(self, mock_aiormq):
        conn1 = SharedConnection(
            "amqp://admin@example.com",
            open_retry_policy=RetryPolicy(delays=[0]),
            reopen_retry_policy=RetryPolicy(delays=[1, 2]),
        )
        conn2 = SharedConnection(
            "amqp://admin@example.com",
            open_retry_policy=RetryPolicy(delays=[1]),
            reopen_retry_policy=RetryPolicy(delays=[1, 2, 3]),
        )

        assert conn1._conn != conn2._conn

        await conn1.open()

        assert conn1.is_open is True
        assert conn2.is_open is False

        await conn1.close()
        await conn2.close()

    @pytest.mark.asyncio
    async def test_new_channel_passes_timeout(self, mock_aiormq):
        conn = SharedConnection("amqp://admin@example.com")
        with mock.patch.object(conn, "open", wraps=conn.open) as open_mock:
            await conn.new_channel(timeout=42)
            open_mock.assert_awaited_once_with(timeout=42)
        await conn.close()

    @pytest.mark.asyncio
    async def test_channel_passes_timeout(self, mock_aiormq):
        conn = SharedConnection("amqp://admin@example.com")
        await conn.open()
        with mock.patch.object(conn, "open", wraps=conn.open) as open_mock:
            await conn.channel(timeout=42)
            open_mock.assert_awaited_once_with(timeout=42)
        await conn.close()


class TestEnvVarFunctions:
    def test_env_var_as_bool_default(self, monkeypatch):
        monkeypatch.delenv("TEST_VAR", raising=False)

        result = _env_var_as_bool("TEST_VAR", False)
        assert result is False

    def test_env_var_as_bool_true_values(self, monkeypatch):
        for value in ["1", "true", "yes", "y", "True", "YES", "Y"]:
            monkeypatch.setenv("TEST_VAR", value)
            assert _env_var_as_bool("TEST_VAR", False) is True

    def test_env_var_as_bool_false_values(self, monkeypatch):
        for value in ["0", "false", "no", "n", "False", "NO", "N"]:
            monkeypatch.setenv("TEST_VAR", value)
            assert _env_var_as_bool("TEST_VAR", True) is False

    def test_env_var_as_bool_invalid(self, monkeypatch):
        monkeypatch.setenv("TEST_VAR", "invalid")

        assert _env_var_as_bool("TEST_VAR", False) is False

    def test_as_int_or_none_valid(self):
        assert _as_int_or_none("10") == 10
        assert _as_int_or_none(10) == 10
        assert _as_int_or_none(10.5) == 10

    def test_as_int_or_none_invalid(self):
        assert _as_int_or_none("invalid") is None
        assert _as_int_or_none(None) is None

    def test_env_var_as_int_default(self, monkeypatch):
        monkeypatch.delenv("TEST_VAR", raising=False)

        result = _env_var_as_int("TEST_VAR", 42)
        assert result == 42

    def test_env_var_as_int_valid(self, monkeypatch):
        for value in ["0", "1", "10", "100", "-5"]:
            monkeypatch.setenv("TEST_VAR", value)
            assert _env_var_as_int("TEST_VAR", 0) == int(value)

    def test_env_var_as_int_invalid(self, monkeypatch):
        monkeypatch.setenv("TEST_VAR", "invalid")

        with pytest.raises(
            ValueError,
            match="invalid value 'invalid' for environment variable TEST_VAR",
        ):
            _env_var_as_int("TEST_VAR", 0)
