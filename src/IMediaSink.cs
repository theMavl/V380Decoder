namespace V380Decoder.src
{
    public interface IMediaSink
    {
        void PushVideo(FrameData frame);
        void PushAudio(FrameData frame);
        void Reset();
    }
}
