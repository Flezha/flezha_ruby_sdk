# frozen_string_literal: true

module Flezha
  # The replay-stable identifier of a cell.
  #
  # The algorithm is fixed by the SDK contract rather than by a library, so
  # every SDK derives the same value from the same cell: FNV-1a over
  # `identity | change seq | per-invocation ordinal`, where the identity is
  # the bytes the core hands with the cell's first activation.
  module Identity
    FNV128_OFFSET_BASIS = 0x6c62272e07bb014262b821756295c58d
    FNV128_PRIME = 0x0000000001000000000000000000013b
    MASK128 = (1 << 128) - 1

    module_function

    def fnv1a128(seed, bytes)
      hash = seed
      bytes.each_byte do |byte|
        hash = (hash ^ byte) & MASK128
        hash = (hash * FNV128_PRIME) & MASK128
      end
      hash
    end

    # Thirty-two lowercase hex characters. Repeated calls in one invocation
    # differ; the n-th call of a replayed invocation equals the n-th call of the
    # original, which is what makes it the replacement for a random UUID.
    def stable_id(identity, change_seq, ordinal)
      hash = fnv1a128(FNV128_OFFSET_BASIS, identity)
      hash = fnv1a128(hash, [change_seq & 0xffffffffffffffff].pack("Q>"))
      hash = fnv1a128(hash, [ordinal & 0xffffffff].pack("N"))
      format("%032x", hash)
    end
  end
end
