defmodule Stompex do
  # CHANGE: Replace 'use Connection' with 'use GenServer'
  # Connection is a specialized behavior for managing connections with automatic
  # reconnection. GenServer is the standard OTP behavior that Connection builds upon.
  use GenServer
  use Stompex.Api
  require Logger

  import Stompex.FrameBuilder

  @tcp_opts [:binary, active: false]

  # CHANGE: Add a default reconnect interval since Connection handled this automatically
  @reconnect_interval 1000

  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  # CHANGE: Implement init/1 callback required by GenServer
  # This merges the Connection behavior's initialization with Api module's init logic
  @impl true
  def init(opts) do
    # CHANGE: Handle both keyword list and tuple initialization
    # This maintains compatibility with the Api module
    {host, port, login, passcode, headers, timeout, calling_process} =
      case opts do
        # When called from Api module with individual parameters
        [host: h, port: p, login: l, passcode: pw, headers: hdrs, timeout: t, calling_process: cp] ->
          {h, p, l, pw, hdrs, t, cp}
        # When called with keyword list
        opts when is_list(opts) ->
          {
            opts[:host] || "localhost",
            opts[:port] || 61613,
            opts[:login] || "",
            opts[:passcode] || "",
            opts[:headers] || %{},
            opts[:timeout] || 10_000,
            opts[:calling_process] || self()
          }
      end

    # CHANGE: Merge login/passcode into headers as per original Api module logic
    headers = Map.merge(%{"accept-version" => "1.2", "login" => login, "passcode" => passcode}, headers)

    # Build initial state with all required fields
    state = %{
      sock: nil,
      host: host,
      port: port,
      headers: headers,
      timeout: timeout,
      callbacks: %{},
      subscriptions: %{},
      subscription_id: 0,  # CHANGE: Start at 0 to match original
      calling_process: calling_process,
      send_to_caller: false,
      receiver: nil,
      version: Stompex.Validator.normalise_version(headers["accept-version"]),
      # CHANGE: Add reconnect flag to handle reconnection manually
      reconnect: opts[:reconnect] || true,
      # CHANGE: Track connection attempts for backoff
      connection_attempts: 0
    }

    # CHANGE: Use handle_continue to attempt connection after init
    # This replaces Connection's {:connect, :init, state} return
    {:ok, state, {:continue, :connect}}
  end

  # CHANGE: Add handle_continue callback to perform initial connection
  # This is the OTP27 recommended way to do work after init
  @impl true
  def handle_continue(:connect, state) do
    case do_connect(state) do
      {:ok, new_state} ->
        {:noreply, new_state}

      {:error, reason} ->
        Logger.error("Failed to connect: #{inspect(reason)}")
        # CHANGE: Schedule reconnection attempt if enabled
        if state.reconnect do
          schedule_reconnect(state.connection_attempts)
          {:noreply, %{state | connection_attempts: state.connection_attempts + 1}}
        else
          {:stop, {:connection_failed, reason}, state}
        end
    end
  end

  # CHANGE: Extract connection logic into a private function
  # This replaces the Connection.connect/2 callback
  defp do_connect(%{host: host, port: port, timeout: timeout} = state) do
    case :gen_tcp.connect(to_charlist(host), port, @tcp_opts, timeout) do
      {:ok, sock} ->
        stomp_connect(sock, state)

      {:error, reason} = error ->
        Logger.warning("#{__MODULE__}.do_connect error -- reason: #{inspect(reason)}, error: #{inspect(error)}")
        error
    end
  end

  # CHANGE: Modified to return {:ok, state} or {:error, reason} tuples
  # instead of Connection-specific return values
  defp stomp_connect(conn, state) do
    frame =
      connect_frame(state.version)
      |> put_header("host", state[:host])
      |> put_headers(state[:headers])
      |> finish_frame()

    with :ok <- :gen_tcp.send(conn, frame),
         {:ok, receiver} <- Stompex.Receiver.start_link(conn),
         {:ok, frame} <- Stompex.Receiver.receive_frame(receiver) do
      connected_with_frame(frame, %{state | sock: conn, receiver: receiver})
    else
      error ->
        # CHANGE: Return error tuple instead of Connection-specific {:stop, reason}
        {:error, "Error connecting to stomp server. #{inspect(error)}"}
    end
  end

  # CHANGE: Modified return values to use standard {:ok, state} tuples
  defp connected_with_frame(%{cmd: "CONNECTED", headers: headers}, %{receiver: receiver} = state) do
    case headers["version"] do
      nil ->
        Logger.debug("STOMP server supplied no version. Reverting to version 1.0")
        Stompex.Receiver.set_version(receiver, 1.0)
        # CHANGE: Reset connection attempts on successful connection
        {:ok, %{state | version: 1.0, connection_attempts: 0}}

      version ->
        Logger.debug("Stompex using protocol version #{version}")
        Stompex.Receiver.set_version(receiver, version)
        # CHANGE: Reset connection attempts on successful connection
        {:ok, %{state | version: version, connection_attempts: 0}}
    end
  end
  defp connected_with_frame(%{cmd: "ERROR", headers: headers}, _state) do
    error = headers["message"] || "Server rejected connection"
    {:error, error}
  end
  defp connected_with_frame(_frame, _state) do
    {:error, "Server rejected connection"}
  end

  # CHANGE: Add handle_info for reconnection attempts
  # Connection behavior handled this automatically
  @impl true
  def handle_info(:reconnect, state) do
    Logger.info("Attempting to reconnect...")
    handle_continue(:connect, state)
  end

  # CHANGE: Modified to handle connection loss and trigger reconnection
  @impl true
  def handle_info({:receiver, frame}, %{send_to_caller: true, calling_process: process, receiver: receiver} = state) do
    dest = frame.headers["destination"]
    frame = decompress_frame(frame, dest, state)

    send(process, {:stompex, dest, frame})
    Stompex.Receiver.next_frame(receiver)

    {:noreply, state}
  end

  @impl true
  def handle_info({:receiver, frame}, %{send_to_caller: false, callbacks: callbacks, receiver: receiver} = state) do
    dest = frame.headers["destination"]
    frame = decompress_frame(frame, dest, state)

    callbacks
    |> Map.get(dest, [])
    |> Enum.each(fn func -> func.(frame) end)

    Stompex.Receiver.next_frame(receiver)

    {:noreply, state}
  end

  # CHANGE: Add handlers for TCP connection events
  # These replace Connection behavior's automatic handling
  @impl true
  def handle_info({:tcp_closed, _sock}, state) do
    Logger.warning("TCP connection closed")
    handle_connection_loss(state)
  end

  @impl true
  def handle_info({:tcp_error, _sock, reason}, state) do
    Logger.error("TCP error: #{inspect(reason)}")
    handle_connection_loss(state)
  end

  # CHANGE: Implement handle_call for close operation
  # Connection.disconnect is replaced with manual disconnect logic
  @impl true
  def handle_call(:close, from, %{sock: sock, receiver: receiver} = state) when not is_nil(sock) do
    frame =
      disconnect_frame()
      |> finish_frame()

    # CHANGE: Reply directly instead of using Connection.reply
    GenServer.reply(from, :ok)

    # Stop the receiver process
    if receiver, do: GenServer.stop(receiver)

    case :gen_tcp.send(sock, frame) do
      :ok ->
        :gen_tcp.close(sock)
        # CHANGE: Don't reconnect after explicit close
        {:reply, :ok, %{state | sock: nil, receiver: nil, reconnect: false}}

      {:error, _} = error ->
        {:stop, error, error, state}
    end
  end

  # CHANGE: Handle close when not connected
  def handle_call(:close, _from, state) do
    {:reply, :ok, %{state | reconnect: false}}
  end

  @impl true
  def handle_call({:register_callback, destination, func}, _, %{callbacks: callbacks} = state) do
    dest_callbacks = Map.get(callbacks, destination, []) ++ [func]
    callbacks =
      case Map.has_key?(callbacks, destination) do
        true -> %{callbacks | destination => dest_callbacks}
        false -> Map.merge(callbacks, %{destination => dest_callbacks})
      end

    {:reply, :ok, %{state | callbacks: callbacks}}
  end

  @impl true
  def handle_call({:remove_callback, destination, func}, _, %{callbacks: callbacks} = state) do
    dest_callbacks = Map.get(callbacks, destination, [])
    dest_callbacks = List.delete(dest_callbacks, func)

    callbacks =
      cond do
        Map.has_key?(callbacks, destination) && dest_callbacks == [] ->
          Map.delete(callbacks, destination)

        dest_callbacks != [] ->
          %{callbacks | destination => dest_callbacks}

        true ->
          callbacks
      end

    {:reply, :ok, %{state | callbacks: callbacks}}
  end

  @impl true
  def handle_call({:subscribe, destination, headers, opts}, _, %{subscriptions: subscriptions} = state) do
    case Map.has_key?(subscriptions, destination) do
      true ->
        {:reply, {:error, "You have already subscribed to this destination"}, state}

      false ->
        subscribe_to_destination(destination, headers, opts, state)
    end
  end

  @impl true
  def handle_call({:unsubscribe, destination}, _, %{subscriptions: subscriptions} = state) do
    case Map.has_key?(subscriptions, destination) do
      true ->
        unsubscribe_from_destination(destination, state)

      false ->
        {:reply, {:error, "You are not subscribed to this destination"}, state}
    end
  end

  @impl true
  def handle_call({:send, destination, %Stompex.Frame{} = frame}, _, %{sock: sock} = state) do
    frame =
      frame
      |> put_header("destination", destination)
      |> put_header("content-length", byte_size(frame.body))
      |> finish_frame()

    response = :gen_tcp.send(sock, frame)
    {:reply, response, state}
  end

  @impl true
  def handle_call({:send, destination, message}, _, %{sock: sock} = state) do
    frame =
      send_frame()
      |> put_header("destination", destination)
      |> put_header("content-length", byte_size(message))
      |> set_body(message)
      |> finish_frame()

    response = :gen_tcp.send(sock, frame)
    {:reply, response, state}
  end

  @impl true
  def handle_cast({:acknowledge, frame}, %{sock: sock} = state) do
    frame =
      ack_frame()
      |> put_header("message-id", frame.headers["message-id"])
      |> put_header("subscription", frame.headers["subscription"])
      |> finish_frame()

    :gen_tcp.send(sock, frame)

    {:noreply, state}
  end

  @impl true
  def handle_cast({:nack, _frame}, %{version: 1.0} = state) do
    Logger.warning("'NACK' frame was requested, but is not valid for version 1.0 of the STOMP protocol. Ignoring")
    {:noreply, state}
  end

  @impl true
  def handle_cast({:nack, frame}, %{sock: sock} = state) do
    frame =
      nack_frame()
      |> put_header("message-id", frame.headers["message-id"])
      |> put_header("subscription", frame.headers["subscription"])
      |> finish_frame()

    :gen_tcp.send(sock, frame)
    {:noreply, state}
  end

  @impl true
  def handle_cast({:send_to_caller, send}, state) do
    {:noreply, %{state | send_to_caller: send}}
  end

  # Private helper functions

  defp subscribe_to_destination(destination, headers, opts, %{sock: sock, subscription_id: id, subscriptions: subs} = state) do
    frame =
      subscribe_frame()
      |> put_header("id", headers["id"] || id)
      |> put_header("ack", headers["ack"] || "auto")
      |> put_header("destination", destination)

    # CHANGE: Match original increment logic with parentheses
    state = %{state | subscription_id: (id + 1)}

    case :gen_tcp.send(sock, finish_frame(frame)) do
      :ok ->
        subscription = %{
          id: frame.headers[:id],
          ack: frame.headers[:ack],
          compressed: Keyword.get(opts, :compressed, false)
        }

        Stompex.Receiver.next_frame(state[:receiver])

        {:reply, :ok, %{state | subscriptions: Map.merge(subs, %{destination => subscription})}}

      {:error, _} = error ->
        # CHANGE: Fixed return value for GenServer
        {:reply, error, state}
    end
  end

  defp unsubscribe_from_destination(destination, %{sock: sock, subscriptions: subscriptions} = state) do
    subscription = subscriptions[destination]
    frame =
      unsubscribe_frame()
      |> put_header("id", subscription[:id])
      |> finish_frame()

    case :gen_tcp.send(sock, frame) do
      :ok ->
        # CHANGE: Fixed return value - should be {:reply, ...} not {:noreply, ...}
        {:reply, :ok, %{state | subscriptions: Map.delete(subscriptions, destination)}}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  defp decompress_frame(frame, dest, %{subscriptions: subs}) do
    subscription = subs[dest]

    case subscription.compressed do
      true ->
        frame
        |> set_body(:zlib.gunzip(frame.body))

      false ->
        frame
    end
  end

  # CHANGE: Add helper function to handle connection loss
  # This centralizes the reconnection logic that Connection provided
  defp handle_connection_loss(%{receiver: receiver, reconnect: reconnect} = state) do
    # Clean up receiver process if it exists
    if receiver && Process.alive?(receiver), do: GenServer.stop(receiver)

    new_state = %{state | sock: nil, receiver: nil}

    if reconnect do
      # Schedule reconnection with exponential backoff
      schedule_reconnect(new_state.connection_attempts)
      {:noreply, %{new_state | connection_attempts: new_state.connection_attempts + 1}}
    else
      {:stop, :connection_lost, new_state}
    end
  end

  # CHANGE: Add helper to schedule reconnection with exponential backoff
  # Connection behavior provided this automatically
  defp schedule_reconnect(attempts) do
    # Exponential backoff with max of 30 seconds
    delay = min(@reconnect_interval * :math.pow(2, attempts), 30_000) |> round()
    Process.send_after(self(), :reconnect, delay)
  end
end