namespace V380Decoder.src
{
    public class FrameData
    {
        public byte RawType;   // V380 fragment type (video 0x00/0x01/0x28/0x29; audio 0x16/0x1A)
        public uint FrameId;
        public ushort FrameType;
        public ushort FrameRate;
        public ulong Timestamp;
        public byte[] Payload;
        public bool IsKeyframe => RawType == 0x00 || RawType == 0x28;
    }
}
