#include <gst/app/gstappsrc.h>
#include <gst/gst.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BRIDGE_HEADER_SIZE 28
#define MAX_PAYLOAD_SIZE (64U * 1024U * 1024U)
#define PACKET_VIDEO 1
#define PACKET_AUDIO 2
#define FLAG_KEYFRAME 1

typedef struct {
    GstElement *pipeline;
    GstElement *video_src;
    GstElement *audio_src;
    GstBus *bus;
    gboolean first_video;
    gboolean first_audio;
} Bridge;

static uint32_t read_le32(const uint8_t *data)
{
    return ((uint32_t)data[0]) |
           ((uint32_t)data[1] << 8) |
           ((uint32_t)data[2] << 16) |
           ((uint32_t)data[3] << 24);
}

static uint64_t read_le64(const uint8_t *data)
{
    return ((uint64_t)read_le32(data)) |
           ((uint64_t)read_le32(data + 4) << 32);
}

static gboolean read_exact(FILE *stream, uint8_t *destination, size_t length)
{
    size_t offset = 0;
    while (offset < length) {
        size_t count = fread(destination + offset, 1, length - offset, stream);
        if (count == 0)
            return FALSE;
        offset += count;
    }
    return TRUE;
}

static void print_bus_error(GstMessage *message)
{
    GError *error = NULL;
    gchar *details = NULL;
    gst_message_parse_error(message, &error, &details);
    fprintf(stderr, "pipeline error from %s: %s\n",
            GST_OBJECT_NAME(message->src),
            error != NULL ? error->message : "unknown error");
    if (details != NULL && details[0] != '\0')
        fprintf(stderr, "pipeline details: %s\n", details);
    g_clear_error(&error);
    g_free(details);
}

static gboolean pipeline_failed(Bridge *bridge)
{
    GstMessage *message = gst_bus_pop_filtered(
        bridge->bus,
        GST_MESSAGE_ERROR | GST_MESSAGE_EOS);
    if (message == NULL)
        return FALSE;

    if (GST_MESSAGE_TYPE(message) == GST_MESSAGE_ERROR)
        print_bus_error(message);
    else
        fprintf(stderr, "pipeline stopped unexpectedly\n");
    gst_message_unref(message);
    return TRUE;
}

static GstElement *make_element(const char *factory, const char *name)
{
    GstElement *element = gst_element_factory_make(factory, name);
    if (element == NULL)
        fprintf(stderr, "missing GStreamer element: %s\n", factory);
    return element;
}

static gboolean configure_appsrc(
    GstElement *element,
    const char *caps_description)
{
    GstCaps *caps = gst_caps_from_string(caps_description);
    if (caps == NULL)
        return FALSE;

    g_object_set(
        element,
        "is-live", TRUE,
        "format", GST_FORMAT_TIME,
        "block", TRUE,
        "emit-signals", FALSE,
        NULL);
    gst_app_src_set_stream_type(GST_APP_SRC(element), GST_APP_STREAM_TYPE_STREAM);
    gst_app_src_set_caps(GST_APP_SRC(element), caps);
    gst_caps_unref(caps);
    return TRUE;
}

static gboolean create_pipeline(
    Bridge *bridge,
    const char *url,
    const char *video_codec,
    const char *audio_codec,
    int audio_block_align)
{
    const gboolean h265 = strcmp(video_codec, "hevc") == 0 ||
                          strcmp(video_codec, "h265") == 0;
    const gboolean audio_enabled = strcmp(audio_codec, "none") != 0;
    const gboolean adpcm_audio =
        audio_enabled && strcmp(audio_codec, "adpcm_ima_wav") == 0;
    bridge->pipeline = gst_pipeline_new("v380-publisher");
    bridge->video_src = make_element("appsrc", "video-source");
    bridge->audio_src = audio_enabled ? make_element("appsrc", "audio-source") : NULL;
    GstElement *video_queue = make_element("queue", "video-queue");
    GstElement *audio_queue = audio_enabled ? make_element("queue", "audio-queue") : NULL;
    GstElement *video_parser = make_element(h265 ? "h265parse" : "h264parse", "video-parser");
    GstElement *audio_decoder = adpcm_audio
        ? make_element("adpcmdec", "audio-decoder")
        : NULL;
    GstElement *audio_convert = adpcm_audio
        ? make_element("audioconvert", "audio-convert")
        : NULL;
    GstElement *alaw_encoder = adpcm_audio
        ? make_element("alawenc", "alaw-encoder")
        : NULL;
    GstElement *sink = make_element("rtspclientsink", "rtsp-publisher");

    if (bridge->pipeline == NULL || bridge->video_src == NULL ||
        video_queue == NULL || video_parser == NULL || sink == NULL ||
        (audio_enabled && (bridge->audio_src == NULL || audio_queue == NULL)) ||
        (adpcm_audio && audio_decoder == NULL) ||
        (adpcm_audio && (audio_convert == NULL || alaw_encoder == NULL)))
        return FALSE;

    const char *video_caps = h265
        ? "video/x-h265,stream-format=byte-stream,alignment=au"
        : "video/x-h264,stream-format=byte-stream,alignment=au";
    if (!configure_appsrc(bridge->video_src, video_caps))
        return FALSE;
    if (audio_enabled) {
        gchar *audio_caps = adpcm_audio
            ? g_strdup_printf(
                "audio/x-adpcm,layout=dvi,rate=8000,channels=1,block_align=%d",
                audio_block_align)
            : g_strdup("audio/x-alaw,rate=8000,channels=1");
        gboolean configured = configure_appsrc(bridge->audio_src, audio_caps);
        g_free(audio_caps);
        if (!configured)
            return FALSE;
    }

    g_object_set(video_parser, "config-interval", -1, NULL);
    g_object_set(
        sink,
        "location", url,
        /* MediaMTX and the reader own network buffering. Keeping another
           publisher-side time buffer would only add avoidable latency. */
        "latency", 0U,
        "rtx-time", 0U,
        NULL);
    gst_util_set_object_arg(G_OBJECT(sink), "protocols", "tcp");

    gst_bin_add_many(
        GST_BIN(bridge->pipeline),
        bridge->video_src,
        video_queue,
        video_parser,
        sink,
        NULL);
    if (audio_enabled)
        gst_bin_add_many(
            GST_BIN(bridge->pipeline), bridge->audio_src, audio_queue, NULL);
    if (adpcm_audio)
        gst_bin_add(GST_BIN(bridge->pipeline), audio_decoder);
    if (adpcm_audio)
        gst_bin_add_many(GST_BIN(bridge->pipeline), audio_convert, alaw_encoder, NULL);

    if (!gst_element_link_many(bridge->video_src, video_queue, video_parser, sink, NULL)) {
        fprintf(stderr, "unable to link video pipeline\n");
        return FALSE;
    }

    if (audio_enabled) {
        gboolean audio_linked;
        if (adpcm_audio)
            audio_linked = gst_element_link_many(
                bridge->audio_src, audio_queue, audio_decoder,
                audio_convert, alaw_encoder, sink, NULL);
        else
            audio_linked = gst_element_link_many(
                bridge->audio_src, audio_queue, sink, NULL);
        if (!audio_linked) {
            fprintf(stderr, "unable to link audio pipeline\n");
            return FALSE;
        }
    }

    bridge->bus = gst_element_get_bus(bridge->pipeline);
    bridge->first_video = TRUE;
    bridge->first_audio = TRUE;

    GstStateChangeReturn state = gst_element_set_state(bridge->pipeline, GST_STATE_PLAYING);
    if (state == GST_STATE_CHANGE_FAILURE) {
        fprintf(stderr, "unable to start GStreamer pipeline\n");
        return FALSE;
    }
    fprintf(stderr, "publishing video=%s audio=%s to %s\n",
            h265 ? "hevc" : "h264", audio_codec, url);
    return TRUE;
}

static gboolean push_packet(
    Bridge *bridge,
    uint8_t type,
    uint8_t flags,
    uint64_t pts,
    uint64_t duration,
    const uint8_t *payload,
    uint32_t payload_size)
{
    GstElement *source = type == PACKET_VIDEO
        ? bridge->video_src
        : bridge->audio_src;
    gboolean *first = type == PACKET_VIDEO
        ? &bridge->first_video
        : &bridge->first_audio;

    if (source == NULL) {
        fprintf(stderr, "received audio packet for a video-only pipeline\n");
        return FALSE;
    }

    GstBuffer *buffer = gst_buffer_new_allocate(NULL, payload_size, NULL);
    if (buffer == NULL)
        return FALSE;
    gst_buffer_fill(buffer, 0, payload, payload_size);
    GST_BUFFER_PTS(buffer) = (GstClockTime)pts;
    GST_BUFFER_DTS(buffer) = (GstClockTime)pts;
    GST_BUFFER_DURATION(buffer) = (GstClockTime)duration;

    if (*first) {
        GST_BUFFER_FLAG_SET(buffer, GST_BUFFER_FLAG_DISCONT);
        *first = FALSE;
    }
    if (type == PACKET_VIDEO && (flags & FLAG_KEYFRAME) == 0)
        GST_BUFFER_FLAG_SET(buffer, GST_BUFFER_FLAG_DELTA_UNIT);

    GstFlowReturn result = gst_app_src_push_buffer(GST_APP_SRC(source), buffer);
    if (result != GST_FLOW_OK) {
        fprintf(stderr, "appsrc rejected %s buffer: %s\n",
                type == PACKET_VIDEO ? "video" : "audio",
                gst_flow_get_name(result));
        return FALSE;
    }
    return TRUE;
}

static void destroy_pipeline(Bridge *bridge)
{
    if (bridge->video_src != NULL)
        gst_app_src_end_of_stream(GST_APP_SRC(bridge->video_src));
    if (bridge->audio_src != NULL)
        gst_app_src_end_of_stream(GST_APP_SRC(bridge->audio_src));
    if (bridge->pipeline != NULL)
        gst_element_set_state(bridge->pipeline, GST_STATE_NULL);
    if (bridge->bus != NULL)
        gst_object_unref(bridge->bus);
    if (bridge->pipeline != NULL)
        gst_object_unref(bridge->pipeline);
}

int main(int argc, char **argv)
{
    const char *url = NULL;
    const char *video_codec = NULL;
    const char *audio_codec = NULL;
    int audio_block_align = 0;

    for (int index = 1; index + 1 < argc; index += 2) {
        if (strcmp(argv[index], "--url") == 0)
            url = argv[index + 1];
        else if (strcmp(argv[index], "--video") == 0)
            video_codec = argv[index + 1];
        else if (strcmp(argv[index], "--audio") == 0)
            audio_codec = argv[index + 1];
        else if (strcmp(argv[index], "--audio-block-align") == 0)
            audio_block_align = atoi(argv[index + 1]);
        else {
            fprintf(stderr, "unknown argument: %s\n", argv[index]);
            return 2;
        }
    }

    if (url == NULL || video_codec == NULL || audio_codec == NULL) {
        fprintf(stderr, "usage: v380-gst-bridge --url URL --video h264|hevc --audio adpcm_ima_wav|alaw|none --audio-block-align N\n");
        return 2;
    }
    if ((strcmp(video_codec, "h264") != 0 && strcmp(video_codec, "hevc") != 0) ||
        (strcmp(audio_codec, "adpcm_ima_wav") != 0 &&
         strcmp(audio_codec, "alaw") != 0 &&
         strcmp(audio_codec, "none") != 0) ||
        (strcmp(audio_codec, "adpcm_ima_wav") == 0 &&
         (audio_block_align < 64 || audio_block_align > 8192))) {
        fprintf(stderr, "unsupported codec selection\n");
        return 2;
    }

    /* Application arguments were parsed above; do not pass them to GLib's
       option parser as if they were GStreamer command-line switches. */
    gst_init(NULL, NULL);
    Bridge bridge = { 0 };
    if (!create_pipeline(&bridge, url, video_codec, audio_codec, audio_block_align)) {
        destroy_pipeline(&bridge);
        return 1;
    }

    uint8_t header[BRIDGE_HEADER_SIZE];
    int exit_code = 0;
    while (read_exact(stdin, header, sizeof(header))) {
        if (memcmp(header, "V38B", 4) != 0) {
            fprintf(stderr, "invalid bridge packet magic\n");
            exit_code = 1;
            break;
        }

        uint8_t type = header[4];
        uint8_t flags = header[5];
        uint64_t pts = read_le64(header + 8);
        uint64_t duration = read_le64(header + 16);
        uint32_t payload_size = read_le32(header + 24);
        if ((type != PACKET_VIDEO && type != PACKET_AUDIO) ||
            payload_size == 0 || payload_size > MAX_PAYLOAD_SIZE) {
            fprintf(stderr, "invalid bridge packet type=%u size=%u\n", type, payload_size);
            exit_code = 1;
            break;
        }

        uint8_t *payload = malloc(payload_size);
        if (payload == NULL || !read_exact(stdin, payload, payload_size)) {
            fprintf(stderr, "truncated bridge packet\n");
            free(payload);
            exit_code = 1;
            break;
        }

        gboolean accepted = push_packet(
            &bridge, type, flags, pts, duration, payload, payload_size);
        free(payload);
        if (!accepted || pipeline_failed(&bridge)) {
            exit_code = 1;
            break;
        }
    }

    destroy_pipeline(&bridge);
    return exit_code;
}
