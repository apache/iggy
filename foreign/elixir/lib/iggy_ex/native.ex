defmodule IggyEx do
  @moduledoc false
  use Rustler, otp_app: :iggy_ex, crate: "iggy_nif"

  def connect(), do: :erlang.nif_error(:nif_not_loaded)
  def ping(), do: :erlang.nif_error(:nif_not_loaded)
  def login_user(_username, _password), do: :erlang.nif_error(:nif_not_loaded)
  def create_stream(_stream_id, _name), do: :erlang.nif_error(:nif_not_loaded)

  def create_topic(_stream_id, _topic_id, _partitions_count, _name),
    do: :erlang.nif_error(:nif_not_loaded)

  def send_messages(_stream_id, _topic_id, _partitioning, _messages),
    do: :erlang.nif_error(:nif_not_loaded)

    def send_message(_stream_id, _topic_id, _partitioning, _message),
    do: :erlang.nif_error(:nif_not_loaded)
end
