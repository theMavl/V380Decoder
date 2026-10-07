using System.Buffers.Binary;

namespace V380Decoder.src;

/// <summary>Decodes one independent Microsoft/DVI IMA ADPCM mono block.</summary>
public static class ImaAdpcmDecoder
{
    private static readonly int[] StepTable =
    [
        7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 19, 21, 23, 25, 28, 31,
        34, 37, 41, 45, 50, 55, 60, 66, 73, 80, 88, 97, 107, 118, 130, 143,
        157, 173, 190, 209, 230, 253, 279, 307, 337, 371, 408, 449, 494, 544,
        598, 658, 724, 796, 876, 963, 1060, 1166, 1282, 1411, 1552, 1707,
        1878, 2066, 2272, 2499, 2749, 3024, 3327, 3660, 4026, 4428, 4871,
        5358, 5894, 6484, 7132, 7845, 8630, 9493, 10442, 11487, 12635,
        13899, 15289, 16818, 18500, 20350, 22385, 24623, 27086, 29794, 32767
    ];

    private static readonly int[] IndexTable = [-1, -1, -1, -1, 2, 4, 6, 8];

    public static short[] Decode(ReadOnlySpan<byte> block)
    {
        if (block.Length < 4)
            throw new InvalidDataException("IMA ADPCM block must include its 4-byte header");
        int predictor = BinaryPrimitives.ReadInt16LittleEndian(block);
        int index = block[2];
        if (index > 88)
            throw new InvalidDataException($"IMA ADPCM step index {index} is outside 0..88");
        if (block[3] != 0)
            throw new InvalidDataException("IMA ADPCM reserved header byte must be zero");

        var samples = new short[checked(1 + (block.Length - 4) * 2)];
        samples[0] = (short)predictor;
        int output = 1;
        for (int i = 4; i < block.Length; i++)
        {
            DecodeNibble(block[i] & 0x0f, ref predictor, ref index, samples, ref output);
            DecodeNibble(block[i] >> 4, ref predictor, ref index, samples, ref output);
        }
        return samples;
    }

    private static void DecodeNibble(int nibble, ref int predictor, ref int index,
        short[] output, ref int outputIndex)
    {
        int step = StepTable[index];
        // DVI/FFmpeg rounds once after combining the nibble bits. Rounding
        // each shifted term separately differs from the WAV reference.
        int difference = ((2 * (nibble & 7) + 1) * step) >> 3;
        predictor += (nibble & 8) != 0 ? -difference : difference;
        predictor = Math.Clamp(predictor, short.MinValue, short.MaxValue);
        index = Math.Clamp(index + IndexTable[nibble & 7], 0, 88);
        output[outputIndex++] = (short)predictor;
    }
}
