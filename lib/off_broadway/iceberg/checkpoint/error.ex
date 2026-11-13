defmodule OffBroadway.Iceberg.Checkpoint.Error do
  @moduledoc """
  A checkpoint that could not be loaded or written. A producer fails its scan with it,
  so it is the `:reason` of the `[:off_broadway_iceberg, :scan, :exception]` event.

  `:reason` is one of:

  - `:incompatible` - A different job, or a slice with different rows, is already
    recorded under the same name. Nothing is overwritten.
  - `:invalid` - A record that this version cannot read, or that is not where it should
    be.
  - `:unauthorized` - The storage refused the credentials, or they could not be fetched.
  - `:not_found` - The storage does not exist, such as a bucket that is missing.
  - `:unavailable` - The storage could not be reached, or failed. A write that fails this
    way may still have been stored.
  """

  defexception [:reason, :message]

  @type t :: %__MODULE__{
          reason: :incompatible | :invalid | :unauthorized | :not_found | :unavailable,
          message: String.t()
        }
end
