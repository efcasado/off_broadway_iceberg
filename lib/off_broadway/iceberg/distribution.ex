defmodule OffBroadway.Iceberg.Distribution do
  @moduledoc false

  # Coordinates the producers of a distributed scan, on every connected node, through an
  # OTP :pg scope. Nothing is stored outside the producers themselves:
  #
  # - Membership: every producer of a scan joins the scan's group, and is told when
  #   others join or leave.
  # - Ownership: each slice belongs to the member that wins rendezvous hashing over the
  #   current members, so a change of members only moves the slices of the members that
  #   joined or left.
  # - Progress: a member that finishes a slice announces it to the others, and members
  #   send what they know to every member that joins. Finished slices only accumulate,
  #   so merging is a union.

  @scope __MODULE__

  @typedoc "Finished slices, with their `{failed_batches, batches, rows}`."
  @type done :: %{non_neg_integer() => {non_neg_integer(), non_neg_integer(), non_neg_integer()}}

  @doc """
  The child spec of the :pg scope, started by the application on every node.
  """
  def child_spec(_arg), do: %{id: @scope, start: {:pg, :start_link, [@scope]}}

  @doc """
  Joins `group` and returns the monitor reference and the current members.
  """
  @spec join(term()) :: {reference(), [pid()]}
  def join(group) do
    :ok = :pg.join(@scope, group, self())
    :pg.monitor(@scope, group)
  end

  @doc """
  Leaves `group` and stops watching it.
  """
  @spec leave(term(), reference()) :: :ok
  def leave(group, monitor) do
    _ = :pg.demonitor(@scope, monitor)
    _ = :pg.leave(@scope, group, self())
    :ok
  end

  @doc """
  The member that owns `slice`. Members agree on it once they agree on the members:
  ties are broken by pid, which compares the same on every node.
  """
  @spec owner(term(), non_neg_integer(), [pid()]) :: pid()
  def owner(group, slice, members) do
    Enum.max_by(members, &{:erlang.phash2({group, slice, &1}), &1})
  end

  @doc """
  Identifies a plan by its slices: SHA-256 over the paths of their data files, slice by
  slice, in order. Every slice, and every path in it, is length-prefixed, so that no two
  plans encode alike.
  """
  @spec fingerprint([[String.t()]]) :: binary()
  def fingerprint(slices) do
    encoded =
      for paths <- slices do
        [<<length(paths)::32>> | Enum.map(paths, &[<<byte_size(&1)::32>>, &1])]
      end

    :crypto.hash(:sha256, [<<length(slices)::32>> | encoded])
  end

  @doc """
  Sends finished slices to `members`, except the caller.
  """
  @spec announce([pid()], term(), done()) :: :ok
  def announce(members, group, done) do
    for member <- members, member != self(), do: send(member, {__MODULE__, group, {:done, done}})
    :ok
  end

  @doc """
  Merges what two members know. A slice read twice can have two sets of counts, so the
  smaller one wins, the same on every member.
  """
  @spec merge(done(), done()) :: done()
  def merge(done, other), do: Map.merge(done, other, fn _slice, a, b -> min(a, b) end)
end
