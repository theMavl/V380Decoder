#include <gst/app/gstappsrc.h>
#include <gst/gst.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define BRIDGE_HEADER_SIZE 28
#define MAX_PAYLOAD_SIZE (64U * 1024U * 1024U)
#define PACKET_VIDEO 1
#define PACKET_AUDIO 2
#define FLAG_KEYFRAME 1
#define FLAG_DISCONT 2
#define AUDIO_HARD_NS (280U * GST_MSECOND)
#define VIDEO_HARD_NS (500U * GST_MSECOND)
#define NATIVE_QUEUE_SAFETY_PACKETS 64

typedef struct {
    uint8_t type;
    uint8_t flags;
    uint64_t pts;
    uint64_t duration;
    uint32_t payload_size;
    uint8_t *payload;
    gint64 arrival_us;
} Packet;

typedef struct {
    GMutex mutex;
    GCond ready;
    GQueue packets;
    const char *name;
    gboolean video;
    gboolean stopped;
    gboolean waiting_for_keyframe;
    gboolean seen_pts;
    uint64_t last_input_pts;
    uint64_t last_output_pts;
    guint64 ingress;
    guint64 emitted;
    guint64 dropped_overflow;
    guint64 dropped_waiting;
    guint64 rejected_pts;
    guint64 discontinuities;
    guint64 slow_pushes;
    guint64 max_push_us;
    guint64 max_age_ms;
    gint64 last_audio_overflow_log_us;
} PacketQueue;

typedef struct {
    GstElement *pipeline;
    GstElement *video_src;
    GstElement *audio_src;
    GstElement *video_gst_queue;
    GstElement *audio_gst_queue;
    GstBus *bus;
    GMainLoop *loop;
    GThread *loop_thread;
    GThread *video_feeder;
    GThread *audio_feeder;
    PacketQueue video_queue;
    PacketQueue audio_queue;
    gint failed;
    gint shutting_down;
    gboolean first_video;
    gboolean first_audio;
    /* Test injection replaces the blocking downstream call, not the feeder or
       queues. Production leaves this NULL and uses GstAppSrc. */
    gboolean (*consumer)(gpointer context, const Packet *packet);
    gpointer consumer_context;
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

static void packet_free(gpointer value)
{
    Packet *packet = value;
    if (packet == NULL) return;
    free(packet->payload);
    free(packet);
}

static void queue_init(PacketQueue *queue, const char *name, gboolean video)
{
    memset(queue, 0, sizeof(*queue));
    g_mutex_init(&queue->mutex);
    g_cond_init(&queue->ready);
    g_queue_init(&queue->packets);
    queue->name = name;
    queue->video = video;
}

static void queue_clear(PacketQueue *queue);

static void queue_stop(PacketQueue *queue)
{
    g_mutex_lock(&queue->mutex);
    queue->stopped = TRUE;
    queue_clear(queue);
    g_cond_broadcast(&queue->ready);
    g_mutex_unlock(&queue->mutex);
}

static void queue_clear(PacketQueue *queue)
{
    while (!g_queue_is_empty(&queue->packets))
        packet_free(g_queue_pop_head(&queue->packets));
}

static void queue_destroy(PacketQueue *queue)
{
    g_mutex_lock(&queue->mutex);
    queue_clear(queue);
    g_mutex_unlock(&queue->mutex);
    g_cond_clear(&queue->ready);
    g_mutex_clear(&queue->mutex);
}

/* The stdin reader calls this only for a short mutex hold. It never calls
   GStreamer and never waits for a track's downstream pipeline. */
static gboolean queue_push(PacketQueue *queue, Packet *packet)
{
    if (packet == NULL) return FALSE;
    g_mutex_lock(&queue->mutex);
    if (queue->stopped) {
        g_mutex_unlock(&queue->mutex);
        packet_free(packet);
        return FALSE;
    }
    queue->ingress++;
    if (queue->seen_pts && packet->pts <= queue->last_input_pts) {
        queue->rejected_pts++;
        fprintf(stderr, "[BRIDGE-PTS] track=%s reason=backward_or_duplicate pts=%" G_GUINT64_FORMAT
                " previous=%" G_GUINT64_FORMAT " rejected=%" G_GUINT64_FORMAT "\n",
                queue->name, packet->pts, queue->last_input_pts, queue->rejected_pts);
        g_mutex_unlock(&queue->mutex);
        packet_free(packet);
        return FALSE;
    }
    queue->last_input_pts = packet->pts;
    queue->seen_pts = TRUE;

    if (queue->video && queue->waiting_for_keyframe &&
        (packet->flags & FLAG_KEYFRAME) == 0) {
        queue->dropped_waiting++;
        if (queue->dropped_waiting == 1 || queue->dropped_waiting % 30 == 0)
            fprintf(stderr, "[BRIDGE-DROP] track=video reason=waiting_for_keyframe dropped=%"
                    G_GUINT64_FORMAT " pts=%" G_GUINT64_FORMAT "\n",
                    queue->dropped_waiting, packet->pts);
        g_mutex_unlock(&queue->mutex);
        packet_free(packet);
        return TRUE;
    }

    guint64 media_age = 0;
    if (queue->video) {
        Packet *oldest = g_queue_peek_head(&queue->packets);
        if (oldest != NULL && packet->pts > oldest->pts)
            media_age = packet->pts - oldest->pts;
    } else {
        for (GList *node = queue->packets.head; node != NULL; node = node->next) {
            Packet *queued = node->data;
            media_age += queued->duration;
        }
        media_age += packet->duration;
    }
    gboolean overflow = g_queue_get_length(&queue->packets) >= NATIVE_QUEUE_SAFETY_PACKETS ||
        media_age > (queue->video ? VIDEO_HARD_NS : AUDIO_HARD_NS);
    if (overflow) {
        guint dropped = g_queue_get_length(&queue->packets);
        queue_clear(queue);
        queue->dropped_overflow += dropped;
        if (queue->video && (packet->flags & FLAG_KEYFRAME) == 0) {
            queue->waiting_for_keyframe = TRUE;
            queue->dropped_overflow++;
            fprintf(stderr, "[BRIDGE-DROP] track=video reason=gop_overflow dropped=%u waiting_for_keyframe\n",
                    dropped + 1);
            g_mutex_unlock(&queue->mutex);
            packet_free(packet);
            return TRUE;
        }
        packet->flags |= FLAG_DISCONT;
        queue->discontinuities++;
        if (queue->video) {
            fprintf(stderr, "[BRIDGE-DROP] track=video reason=queue_overflow dropped=%u latest_pts=%"
                    G_GUINT64_FORMAT " discont=1\n", dropped, packet->pts);
        } else {
            gint64 now_us = g_get_monotonic_time();
            if (queue->last_audio_overflow_log_us == 0 ||
                now_us - queue->last_audio_overflow_log_us >= G_TIME_SPAN_SECOND) {
                queue->last_audio_overflow_log_us = now_us;
                fprintf(stderr, "[BRIDGE-DROP] track=audio reason=queue_overflow dropped=%u total_dropped=%"
                        G_GUINT64_FORMAT " latest_pts=%" G_GUINT64_FORMAT " discont=1\n",
                        dropped, queue->dropped_overflow, packet->pts);
            }
        }
    }
    if (queue->video && queue->waiting_for_keyframe) {
        queue->waiting_for_keyframe = FALSE;
        packet->flags |= FLAG_DISCONT;
        queue->discontinuities++;
        fprintf(stderr, "[BRIDGE-RECOVERY] track=video keyframe_pts=%" G_GUINT64_FORMAT
                " discont=1 dropped=%" G_GUINT64_FORMAT "\n",
                packet->pts, queue->dropped_overflow + queue->dropped_waiting);
    }
    g_queue_push_tail(&queue->packets, packet);
    g_cond_signal(&queue->ready);
    g_mutex_unlock(&queue->mutex);
    return TRUE;
}

static Packet *queue_pop(PacketQueue *queue)
{
    g_mutex_lock(&queue->mutex);
    while (g_queue_is_empty(&queue->packets) && !queue->stopped)
        g_cond_wait(&queue->ready, &queue->mutex);
    Packet *packet = g_queue_pop_head(&queue->packets);
    if (packet != NULL) {
        gint64 elapsed_us = g_get_monotonic_time() - packet->arrival_us;
        guint64 age_ms = elapsed_us > 0 ? (guint64)elapsed_us / 1000 : 0;
        queue->max_age_ms = MAX(queue->max_age_ms, age_ms);
    }
    g_mutex_unlock(&queue->mutex);
    return packet;
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

static GstElement *make_element(const char *factory, const char *name)
{
    GstElement *element = gst_element_factory_make(factory, name);
    if (element == NULL)
        fprintf(stderr, "missing GStreamer element: %s\n", factory);
    return element;
}

static gboolean configure_appsrc(
    GstElement *element,
    const char *caps_description,
    guint64 max_time,
    guint64 max_buffers)
{
    GstCaps *caps = gst_caps_from_string(caps_description);
    if (caps == NULL)
        return FALSE;

    g_object_set(
        element,
        "is-live", TRUE,
        "format", GST_FORMAT_TIME,
        "block", TRUE,
        "max-time", max_time,
        "max-buffers", max_buffers,
        "max-bytes", (guint64)0,
        "emit-signals", FALSE,
        NULL);
    gst_util_set_object_arg(G_OBJECT(element), "leaky-type", "none");
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
    int audio_block_align,
    gboolean test_mode)
{
    const gboolean h265 = strcmp(video_codec, "hevc") == 0 ||
                          strcmp(video_codec, "h265") == 0;
    const gboolean audio_enabled = strcmp(audio_codec, "none") != 0;
    const gboolean adpcm_audio =
        audio_enabled && strcmp(audio_codec, "adpcm_ima_wav") == 0;
    const gboolean pcm_audio =
        audio_enabled && strcmp(audio_codec, "pcm_s16le") == 0;
    bridge->pipeline = gst_pipeline_new("v380-publisher");
    bridge->video_src = make_element("appsrc", "video-source");
    bridge->audio_src = audio_enabled ? make_element("appsrc", "audio-source") : NULL;
    GstElement *video_queue = make_element("queue", "video-queue");
    GstElement *audio_queue = audio_enabled ? make_element("queue", "audio-queue") : NULL;
    bridge->video_gst_queue = video_queue;
    bridge->audio_gst_queue = audio_queue;
    GstElement *video_parser = make_element(h265 ? "h265parse" : "h264parse", "video-parser");
    GstElement *audio_decoder = adpcm_audio
        ? make_element("adpcmdec", "audio-decoder")
        : NULL;
    GstElement *audio_convert = pcm_audio || adpcm_audio
        ? make_element("audioconvert", "audio-convert")
        : NULL;
    GstElement *alaw_encoder = pcm_audio || adpcm_audio
        ? make_element("alawenc", "alaw-encoder")
        : NULL;
    GstElement *sink = make_element(test_mode ? "fakesink" : "rtspclientsink",
                                    test_mode ? "video-test-sink" : "rtsp-publisher");
    GstElement *audio_test_sink = test_mode && audio_enabled
        ? make_element("fakesink", "audio-test-sink")
        : NULL;

    if (bridge->pipeline == NULL || bridge->video_src == NULL ||
        video_queue == NULL || video_parser == NULL || sink == NULL ||
        (test_mode && audio_enabled && audio_test_sink == NULL) ||
        (audio_enabled && (bridge->audio_src == NULL || audio_queue == NULL)) ||
        (adpcm_audio && audio_decoder == NULL) ||
        (adpcm_audio && (audio_convert == NULL || alaw_encoder == NULL)) ||
        (pcm_audio && (audio_convert == NULL || alaw_encoder == NULL)))
        return FALSE;

    const char *video_caps = h265
        ? "video/x-h265,stream-format=byte-stream,alignment=au"
        : "video/x-h264,stream-format=byte-stream,alignment=au";
    if (!configure_appsrc(bridge->video_src, video_caps,
                          (guint64)200 * GST_MSECOND, 1))
        return FALSE;
    if (audio_enabled) {
        gchar *audio_caps = adpcm_audio
            ? g_strdup_printf("audio/x-adpcm,layout=dvi,rate=8000,channels=1,block_align=%d", audio_block_align)
            : pcm_audio
                ? g_strdup("audio/x-raw,format=S16LE,layout=interleaved,rate=8000,channels=1")
                : g_strdup("audio/x-alaw,rate=8000,channels=1");
        gboolean configured = configure_appsrc(bridge->audio_src, audio_caps,
                                               (guint64)70 * GST_MSECOND, 1);
        g_free(audio_caps);
        if (!configured)
            return FALSE;
    }

    g_object_set(video_parser, "config-interval", -1, NULL);
    g_object_set(video_queue,
        "max-size-time", (guint64)200 * GST_MSECOND,
        "max-size-buffers", 1U,
        "max-size-bytes", 0U,
        NULL);
    gst_util_set_object_arg(G_OBJECT(video_queue), "leaky", "no");
    if (audio_enabled) {
        g_object_set(audio_queue,
            "max-size-time", (guint64)70 * GST_MSECOND,
            "max-size-buffers", 1U,
            "max-size-bytes", 0U,
            NULL);
        gst_util_set_object_arg(G_OBJECT(audio_queue), "leaky", "no");
    }
    if (test_mode) {
        g_object_set(sink, "sync", FALSE, "async", FALSE, NULL);
        if (audio_test_sink != NULL)
            g_object_set(audio_test_sink, "sync", FALSE, "async", FALSE, NULL);
    } else {
        g_object_set(
            sink,
            "location", url,
            /* MediaMTX and the reader own network buffering. Keeping another
               publisher-side time buffer would only add avoidable latency. */
            "latency", 0U,
            "rtx-time", 0U,
            NULL);
        gst_util_set_object_arg(G_OBJECT(sink), "protocols", "tcp");
    }

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
    if (audio_test_sink != NULL)
        gst_bin_add(GST_BIN(bridge->pipeline), audio_test_sink);
    if (pcm_audio)
        gst_bin_add_many(GST_BIN(bridge->pipeline), audio_convert, alaw_encoder, NULL);
    if (adpcm_audio) {
        gst_bin_add_many(GST_BIN(bridge->pipeline), audio_decoder, audio_convert, alaw_encoder, NULL);
    }

    if (!gst_element_link_many(bridge->video_src, video_queue, video_parser, sink, NULL)) {
        fprintf(stderr, "unable to link video pipeline\n");
        return FALSE;
    }

    if (audio_enabled) {
        gboolean audio_linked;
        if (pcm_audio)
            audio_linked = gst_element_link_many(
                bridge->audio_src, audio_queue,
                audio_convert, alaw_encoder,
                test_mode ? audio_test_sink : sink, NULL);
        else if (adpcm_audio) {
            audio_linked = gst_element_link_many(
                bridge->audio_src, audio_queue, audio_decoder,
                audio_convert, alaw_encoder,
                test_mode ? audio_test_sink : sink, NULL);
        }
        else
            audio_linked = gst_element_link_many(
                bridge->audio_src, audio_queue,
                test_mode ? audio_test_sink : sink, NULL);
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
    if (test_mode)
        fprintf(stderr, "validated pipeline video=%s audio=%s\n",
                h265 ? "hevc" : "h264", audio_codec);
    else
        fprintf(stderr, "publishing video=%s audio=%s to %s\n",
                h265 ? "hevc" : "h264", audio_codec, url);
    return TRUE;
}

static gboolean push_packet(
    Bridge *bridge,
    const Packet *packet)
{
    if (bridge->consumer != NULL)
        return bridge->consumer(bridge->consumer_context, packet);
    GstElement *source = packet->type == PACKET_VIDEO
        ? bridge->video_src
        : bridge->audio_src;
    gboolean *first = packet->type == PACKET_VIDEO
        ? &bridge->first_video
        : &bridge->first_audio;

    if (source == NULL) {
        fprintf(stderr, "received audio packet for a video-only pipeline\n");
        return FALSE;
    }

    GstBuffer *buffer = gst_buffer_new_allocate(NULL, packet->payload_size, NULL);
    if (buffer == NULL)
        return FALSE;
    gst_buffer_fill(buffer, 0, packet->payload, packet->payload_size);
    GST_BUFFER_PTS(buffer) = (GstClockTime)packet->pts;
    GST_BUFFER_DTS(buffer) = (GstClockTime)packet->pts;
    GST_BUFFER_DURATION(buffer) = (GstClockTime)packet->duration;

    if (*first) {
        GST_BUFFER_FLAG_SET(buffer, GST_BUFFER_FLAG_DISCONT);
        *first = FALSE;
    }
    if ((packet->flags & FLAG_DISCONT) != 0)
        GST_BUFFER_FLAG_SET(buffer, GST_BUFFER_FLAG_DISCONT);
    if (packet->type == PACKET_VIDEO && (packet->flags & FLAG_KEYFRAME) == 0)
        GST_BUFFER_FLAG_SET(buffer, GST_BUFFER_FLAG_DELTA_UNIT);

    GstFlowReturn result = gst_app_src_push_buffer(GST_APP_SRC(source), buffer);
    if (result != GST_FLOW_OK) {
        if (g_atomic_int_get(&bridge->shutting_down) && result == GST_FLOW_FLUSHING) {
            fprintf(stderr, "[BRIDGE] track=%s flow=FLUSHING during shutdown\n",
                    packet->type == PACKET_VIDEO ? "video" : "audio");
            return TRUE;
        }
        fprintf(stderr, "appsrc rejected %s buffer: %s\n",
                packet->type == PACKET_VIDEO ? "video" : "audio",
                gst_flow_get_name(result));
        return FALSE;
    }
    return TRUE;
}

static gboolean bus_message(GstBus *bus, GstMessage *message, gpointer data)
{
    (void)bus;
    Bridge *bridge = data;
    if (GST_MESSAGE_TYPE(message) == GST_MESSAGE_ERROR) {
        print_bus_error(message);
        g_atomic_int_set(&bridge->failed, 1);
        if (!g_atomic_int_get(&bridge->shutting_down))
            _exit(1); /* stdin may be blocked in fread; report failure promptly */
    } else if (GST_MESSAGE_TYPE(message) == GST_MESSAGE_EOS &&
               !g_atomic_int_get(&bridge->shutting_down)) {
        fprintf(stderr, "pipeline ended unexpectedly\n");
        g_atomic_int_set(&bridge->failed, 1);
        _exit(1);
    }
    return TRUE;
}

static void print_queue_metrics(PacketQueue *queue, GstElement *source, GstElement *gst_queue)
{
    guint64 app_time = 0, gst_time = 0;
    guint64 app_buffers = 0;
    guint gst_buffers = 0;
    if (source != NULL)
        g_object_get(source, "current-level-time", &app_time,
                     "current-level-buffers", &app_buffers, NULL);
    if (gst_queue != NULL)
        g_object_get(gst_queue, "current-level-time", &gst_time,
                     "current-level-buffers", &gst_buffers, NULL);
    g_mutex_lock(&queue->mutex);
    Packet *oldest = g_queue_peek_head(&queue->packets);
    Packet *newest = g_queue_peek_tail(&queue->packets);
    guint64 wall_age_ms = oldest != NULL ?
        (guint64)MAX((gint64)0, g_get_monotonic_time() - oldest->arrival_us) / 1000 : 0;
    guint64 media_age_ns = 0;
    if (oldest != NULL) {
        if (queue->video) {
            if (newest->pts > oldest->pts) media_age_ns = newest->pts - oldest->pts;
        } else {
            for (GList *node = queue->packets.head; node != NULL; node = node->next) {
                Packet *item = node->data;
                media_age_ns += item->duration;
            }
        }
    }
    fprintf(stderr, "[BRIDGE-METRICS] track=%s ingress=%" G_GUINT64_FORMAT
            " emitted=%" G_GUINT64_FORMAT " drop_overflow=%" G_GUINT64_FORMAT
            " drop_waiting=%" G_GUINT64_FORMAT " rejected_pts=%" G_GUINT64_FORMAT
            " discont=%" G_GUINT64_FORMAT " native_count=%u native_media_age_ms=%.3f"
            " native_wall_age_ms=%" G_GUINT64_FORMAT " native_max_wall_age_ms=%" G_GUINT64_FORMAT
            " appsrc_count=%" G_GUINT64_FORMAT " appsrc_age_ms=%.3f gst_count=%u gst_age_ms=%.3f"
            " input_pts=%" G_GUINT64_FORMAT " output_pts=%" G_GUINT64_FORMAT
            " slow_pushes=%" G_GUINT64_FORMAT " max_push_ms=%.3f\n",
            queue->name, queue->ingress, queue->emitted, queue->dropped_overflow,
            queue->dropped_waiting, queue->rejected_pts, queue->discontinuities,
            g_queue_get_length(&queue->packets), media_age_ns / 1000000.0,
            wall_age_ms, queue->max_age_ms, app_buffers, app_time / 1000000.0,
            gst_buffers, gst_time / 1000000.0, queue->last_input_pts,
            queue->last_output_pts, queue->slow_pushes, queue->max_push_us / 1000.0);
    g_mutex_unlock(&queue->mutex);
}

static gboolean metrics_tick(gpointer data)
{
    Bridge *bridge = data;
    print_queue_metrics(&bridge->video_queue, bridge->video_src, bridge->video_gst_queue);
    if (bridge->audio_src != NULL)
        print_queue_metrics(&bridge->audio_queue, bridge->audio_src, bridge->audio_gst_queue);
    return G_SOURCE_CONTINUE;
}

static gpointer event_loop(gpointer data)
{
    Bridge *bridge = data;
    guint watch = gst_bus_add_watch(bridge->bus, bus_message, bridge);
    guint timer = g_timeout_add_seconds(10, metrics_tick, bridge);
    g_main_loop_run(bridge->loop);
    g_source_remove(watch);
    g_source_remove(timer);
    return NULL;
}

typedef struct {
    Bridge *bridge;
    PacketQueue *queue;
} Feeder;

static gpointer feed_track(gpointer data)
{
    Feeder *feeder = data;
    Bridge *bridge = feeder->bridge;
    PacketQueue *queue = feeder->queue;
    g_free(feeder);
    for (;;) {
        Packet *packet = queue_pop(queue);
        if (packet == NULL) break;
        gint64 start_us = g_get_monotonic_time();
        gboolean accepted = push_packet(bridge, packet);
        guint64 write_us = (guint64)MAX((gint64)0, g_get_monotonic_time() - start_us);
        g_mutex_lock(&queue->mutex);
        queue->max_push_us = MAX(queue->max_push_us, write_us);
        if (write_us > 20000) queue->slow_pushes++;
        if (accepted) {
            queue->emitted++;
            queue->last_output_pts = packet->pts;
        }
        g_cond_broadcast(&queue->ready);
        g_mutex_unlock(&queue->mutex);
        packet_free(packet);
        if (!accepted) {
            if (!g_atomic_int_get(&bridge->shutting_down)) {
                fprintf(stderr, "[BRIDGE-FAIL] track=%s appsrc push failed\n", queue->name);
                g_atomic_int_set(&bridge->failed, 1);
                _exit(1);
            }
            break;
        }
    }
    return NULL;
}

static GThread *start_feeder(Bridge *bridge, PacketQueue *queue)
{
    Feeder *feeder = g_new(Feeder, 1);
    feeder->bridge = bridge;
    feeder->queue = queue;
    return g_thread_new(queue->name, feed_track, feeder);
}

static Packet *test_packet(uint8_t type, uint8_t flags, uint64_t pts, uint64_t duration)
{
    Packet *packet = calloc(1, sizeof(*packet));
    if (packet == NULL) return NULL;
    packet->payload = malloc(1);
    if (packet->payload == NULL) { free(packet); return NULL; }
    packet->payload[0] = 0x55;
    packet->payload_size = 1;
    packet->type = type;
    packet->flags = flags;
    packet->pts = pts;
    packet->duration = duration;
    packet->arrival_us = g_get_monotonic_time();
    return packet;
}

static gboolean expect_packet(PacketQueue *queue, uint64_t pts, gboolean discont)
{
    g_mutex_lock(&queue->mutex);
    gboolean empty = g_queue_is_empty(&queue->packets);
    g_mutex_unlock(&queue->mutex);
    if (empty) {
        fprintf(stderr, "queue self-test found no packet track=%s\n", queue->name);
        return FALSE;
    }
    Packet *packet = queue_pop(queue);
    gboolean passed = packet != NULL && packet->pts == pts &&
        (((packet->flags & FLAG_DISCONT) != 0) == discont);
    if (!passed)
        fprintf(stderr, "queue self-test failed track=%s expected_pts=%" G_GUINT64_FORMAT
                " expected_discont=%d\n", queue->name, pts, discont);
    packet_free(packet);
    return passed;
}

typedef struct {
    GMutex mutex;
    GCond changed;
    uint8_t blocked_track;
    gboolean entered;
    gboolean released;
    uint64_t last_pts[3];
    uint8_t last_flags[3];
    guint counts[3];
} StalledConsumer;

static gboolean controlled_consumer(gpointer data, const Packet *packet)
{
    StalledConsumer *consumer = data;
    g_mutex_lock(&consumer->mutex);
    if (packet->type == consumer->blocked_track && !consumer->released) {
        consumer->entered = TRUE;
        g_cond_broadcast(&consumer->changed);
        while (!consumer->released)
            g_cond_wait(&consumer->changed, &consumer->mutex);
    }
    consumer->counts[packet->type]++;
    consumer->last_pts[packet->type] = packet->pts;
    consumer->last_flags[packet->type] = packet->flags;
    g_mutex_unlock(&consumer->mutex);
    return TRUE;
}

/* Deadlines are deadlock watchdogs; no correctness assertion depends on speed,
   sleep duration, thread scheduling, or when a queue happens to overflow. */
static gboolean wait_emitted(PacketQueue *queue, guint64 count)
{
    gint64 deadline = g_get_monotonic_time() + 5 * G_TIME_SPAN_SECOND;
    g_mutex_lock(&queue->mutex);
    while (queue->emitted < count) {
        if (!g_cond_wait_until(&queue->ready, &queue->mutex, deadline)) break;
    }
    gboolean result = queue->emitted == count;
    g_mutex_unlock(&queue->mutex);
    return result;
}

static gboolean self_test_stall(uint8_t blocked_track)
{
    Bridge bridge = { 0 };
    StalledConsumer consumer = { 0 };
    g_mutex_init(&consumer.mutex);
    g_cond_init(&consumer.changed);
    consumer.blocked_track = blocked_track;
    bridge.consumer = controlled_consumer;
    bridge.consumer_context = &consumer;
    queue_init(&bridge.video_queue, "video-stall-test", TRUE);
    queue_init(&bridge.audio_queue, "audio-stall-test", FALSE);
    bridge.video_feeder = start_feeder(&bridge, &bridge.video_queue);
    bridge.audio_feeder = start_feeder(&bridge, &bridge.audio_queue);
    PacketQueue *stalled = blocked_track == PACKET_AUDIO ? &bridge.audio_queue : &bridge.video_queue;
    PacketQueue *live = blocked_track == PACKET_AUDIO ? &bridge.video_queue : &bridge.audio_queue;
    uint8_t live_track = blocked_track == PACKET_AUDIO ? PACKET_VIDEO : PACKET_AUDIO;
    gboolean passed = queue_push(stalled, test_packet(blocked_track, FLAG_KEYFRAME, 0, 63125000));
    gint64 deadline = g_get_monotonic_time() + 5 * G_TIME_SPAN_SECOND;
    g_mutex_lock(&consumer.mutex);
    while (!consumer.entered)
        if (!g_cond_wait_until(&consumer.changed, &consumer.mutex, deadline)) break;
    passed &= consumer.entered;
    g_mutex_unlock(&consumer.mutex);

    for (guint i = 0; i < 5; i++) {
        passed &= queue_push(live, test_packet(live_track, FLAG_KEYFRAME,
            (uint64_t)i * 63125000, 63125000));
        passed &= wait_emitted(live, i + 1);
    }
    if (blocked_track == PACKET_AUDIO) {
        for (guint i = 1; i <= 4; i++)
            passed &= queue_push(stalled, test_packet(PACKET_AUDIO, 0,
                (uint64_t)i * 63125000, 63125000));
        g_mutex_lock(&stalled->mutex);
        passed &= stalled->dropped_overflow == 0;
        passed &= g_queue_get_length(&stalled->packets) == 4;
        guint64 queued_audio_ns = 0;
        for (GList *node = stalled->packets.head; node != NULL; node = node->next)
            queued_audio_ns += ((Packet *)node->data)->duration;
        passed &= queued_audio_ns == 252500000;
        g_mutex_unlock(&stalled->mutex);
        passed &= queue_push(stalled, test_packet(PACKET_AUDIO, 0, 315625000, 63125000));
    } else {
        passed &= queue_push(stalled, test_packet(PACKET_VIDEO, FLAG_KEYFRAME, 100000000, 0));
        for (guint i = 2; i <= 6; i++)
            passed &= queue_push(stalled, test_packet(PACKET_VIDEO, 0,
                (uint64_t)i * 100000000, 0));
        g_mutex_lock(&stalled->mutex);
        passed &= stalled->dropped_overflow == 0;
        passed &= g_queue_get_length(&stalled->packets) == 6;
        Packet *video_oldest = g_queue_peek_head(&stalled->packets);
        Packet *video_newest = g_queue_peek_tail(&stalled->packets);
        passed &= video_newest->pts - video_oldest->pts == VIDEO_HARD_NS;
        g_mutex_unlock(&stalled->mutex);
        passed &= queue_push(stalled, test_packet(PACKET_VIDEO, 0, 700000000, 0));
        passed &= queue_push(stalled, test_packet(PACKET_VIDEO, 0, 800000000, 0));
        passed &= queue_push(stalled, test_packet(PACKET_VIDEO, FLAG_KEYFRAME, 900000000, 0));
    }
    g_mutex_lock(&stalled->mutex);
    passed &= g_queue_get_length(&stalled->packets) == 1;
    passed &= stalled->emitted == 0 && stalled->discontinuities == 1;
    passed &= stalled->dropped_overflow == (blocked_track == PACKET_AUDIO ? 4 : 7);
    passed &= stalled->dropped_waiting == (blocked_track == PACKET_AUDIO ? 0 : 1);
    g_mutex_unlock(&stalled->mutex);

    g_mutex_lock(&consumer.mutex);
    consumer.released = TRUE;
    g_cond_broadcast(&consumer.changed);
    g_mutex_unlock(&consumer.mutex);
    passed &= wait_emitted(stalled, 2);
    g_mutex_lock(&consumer.mutex);
    passed &= consumer.counts[blocked_track] == 2 && consumer.counts[live_track] == 5;
    passed &= consumer.last_pts[blocked_track] ==
        (blocked_track == PACKET_AUDIO ? 315625000 : 900000000);
    passed &= (consumer.last_flags[blocked_track] & FLAG_DISCONT) != 0;
    if (blocked_track == PACKET_VIDEO)
        passed &= (consumer.last_flags[blocked_track] & FLAG_KEYFRAME) != 0;
    g_mutex_unlock(&consumer.mutex);
    g_mutex_lock(&stalled->mutex);
    passed &= g_queue_is_empty(&stalled->packets);
    g_mutex_unlock(&stalled->mutex);
    passed &= queue_push(stalled, test_packet(blocked_track, FLAG_KEYFRAME,
        blocked_track == PACKET_AUDIO ? 378750000 : 1000000000, 63125000));
    passed &= wait_emitted(stalled, 3);
    g_mutex_lock(&consumer.mutex);
    passed &= consumer.last_pts[blocked_track] ==
        (blocked_track == PACKET_AUDIO ? 378750000 : 1000000000);
    passed &= (consumer.last_flags[blocked_track] & FLAG_DISCONT) == 0;
    g_mutex_unlock(&consumer.mutex);
    queue_stop(&bridge.audio_queue);
    queue_stop(&bridge.video_queue);
    g_thread_join(bridge.audio_feeder);
    g_thread_join(bridge.video_feeder);
    queue_destroy(&bridge.audio_queue);
    queue_destroy(&bridge.video_queue);
    g_cond_clear(&consumer.changed);
    g_mutex_clear(&consumer.mutex);
    fprintf(stderr, "controlled %s feeder stall: %s (other track progressed, live-edge gap/DISCONT/counters)\n",
        blocked_track == PACKET_AUDIO ? "audio" : "video", passed ? "passed" : "FAILED");
    return passed;
}

static int self_test_queues(void)
{
    PacketQueue audio, video;
    queue_init(&audio, "audio-test", FALSE);
    queue_init(&video, "video-test", TRUE);
    gboolean passed = TRUE;

    /* Four complete blocks fit below the 280 ms audio hard limit. */
    passed &= queue_push(&audio, test_packet(PACKET_AUDIO, 0, 0, 63125000));
    passed &= queue_push(&audio, test_packet(PACKET_AUDIO, 0, 63125000, 63125000));
    passed &= queue_push(&audio, test_packet(PACKET_AUDIO, 0, 126250000, 63125000));
    passed &= queue_push(&audio, test_packet(PACKET_AUDIO, 0, 189375000, 63125000));
    passed &= audio.dropped_overflow == 0 && g_queue_get_length(&audio.packets) == 4;
    guint64 audio_age_ns = 0;
    for (GList *node = audio.packets.head; node != NULL; node = node->next)
        audio_age_ns += ((Packet *)node->data)->duration;
    passed &= audio_age_ns == 252500000;
    /* The fifth block crosses 280 ms: retain only it at its source PTS. */
    passed &= queue_push(&audio, test_packet(PACKET_AUDIO, 0, 252500000, 63125000));
    passed &= audio.dropped_overflow == 4 && audio.discontinuities == 1;
    passed &= g_queue_get_length(&audio.packets) == 1;
    passed &= expect_packet(&audio, 252500000, TRUE);
    passed &= queue_push(&audio, test_packet(PACKET_AUDIO, 0, 315625000, 63125000));
    passed &= expect_packet(&audio, 315625000, FALSE);

    /* A video span through 500 ms is accepted; the next delta drops that GOP. */
    passed &= queue_push(&video, test_packet(PACKET_VIDEO, FLAG_KEYFRAME, 0, 0));
    for (guint i = 1; i <= 5; i++)
        passed &= queue_push(&video, test_packet(PACKET_VIDEO, 0, (uint64_t)i * 100000000, 0));
    passed &= video.dropped_overflow == 0 && g_queue_get_length(&video.packets) == 6;
    Packet *video_oldest = g_queue_peek_head(&video.packets);
    Packet *video_newest = g_queue_peek_tail(&video.packets);
    passed &= video_newest->pts - video_oldest->pts == VIDEO_HARD_NS;
    passed &= queue_push(&video, test_packet(PACKET_VIDEO, 0, 600000000, 0));
    passed &= video.dropped_overflow == 7 && video.waiting_for_keyframe;
    passed &= queue_push(&video, test_packet(PACKET_VIDEO, 0, 700000000, 0));
    passed &= video.dropped_waiting == 1;
    passed &= queue_push(&video, test_packet(PACKET_VIDEO, FLAG_KEYFRAME, 800000000, 0));
    passed &= expect_packet(&video, 800000000, TRUE);
    passed &= queue_push(&video, test_packet(PACKET_VIDEO, FLAG_KEYFRAME, 900000000, 0));
    passed &= expect_packet(&video, 900000000, FALSE);

    /* A video stall and its recovery do not block or erase audio. */
    passed &= queue_push(&video, test_packet(PACKET_VIDEO, FLAG_KEYFRAME, 1000000000, 0));
    passed &= queue_push(&audio, test_packet(PACKET_AUDIO, 0, 378750000, 63125000));
    passed &= expect_packet(&audio, 378750000, FALSE);
    passed &= expect_packet(&video, 1000000000, FALSE);
    passed &= !queue_push(&audio, test_packet(PACKET_AUDIO, 0, 378750000, 63125000));
    passed &= audio.rejected_pts == 1 && audio.ingress == 8;

    queue_destroy(&audio);
    queue_destroy(&video);
    passed &= self_test_stall(PACKET_VIDEO);
    passed &= self_test_stall(PACKET_AUDIO);
    if (!passed) return 1;
    fprintf(stderr, "native audio/video queue regression checks passed\n");
    return 0;
}

static void destroy_pipeline(Bridge *bridge);

static int self_test_pipelines(void)
{
    const char *codecs[] = { "h264", "hevc" };
    for (guint i = 0; i < G_N_ELEMENTS(codecs); i++) {
        Bridge bridge = { 0 };
        queue_init(&bridge.video_queue, "video-test", TRUE);
        queue_init(&bridge.audio_queue, "audio-test", FALSE);
        if (!create_pipeline(&bridge, NULL, codecs[i], "pcm_s16le", 0, TRUE)) {
            fprintf(stderr, "pipeline self-test failed for %s + pcm_s16le\n", codecs[i]);
            destroy_pipeline(&bridge);
            return 1;
        }
        destroy_pipeline(&bridge);
    }
    fprintf(stderr, "native H.264/H.265 + PCM pipeline checks passed\n");
    return 0;
}

static void destroy_pipeline(Bridge *bridge)
{
    g_atomic_int_set(&bridge->shutting_down, 1);
    if (bridge->loop != NULL) g_main_loop_quit(bridge->loop);
    if (bridge->loop_thread != NULL) g_thread_join(bridge->loop_thread);
    queue_stop(&bridge->video_queue);
    queue_stop(&bridge->audio_queue);
    if (bridge->pipeline != NULL)
        gst_element_set_state(bridge->pipeline, GST_STATE_NULL);
    if (bridge->video_feeder != NULL) g_thread_join(bridge->video_feeder);
    if (bridge->audio_feeder != NULL) g_thread_join(bridge->audio_feeder);
    print_queue_metrics(&bridge->video_queue, bridge->video_src, bridge->video_gst_queue);
    if (bridge->audio_src != NULL)
        print_queue_metrics(&bridge->audio_queue, bridge->audio_src, bridge->audio_gst_queue);
    queue_destroy(&bridge->video_queue);
    queue_destroy(&bridge->audio_queue);
    if (bridge->loop != NULL) g_main_loop_unref(bridge->loop);
    if (bridge->bus != NULL)
        gst_object_unref(bridge->bus);
    if (bridge->pipeline != NULL)
        gst_object_unref(bridge->pipeline);
}

int main(int argc, char **argv)
{
    if (argc == 2 && strcmp(argv[1], "--self-test-queues") == 0)
        return self_test_queues();
    if (argc == 2 && strcmp(argv[1], "--self-test-pipelines") == 0) {
        gst_init(NULL, NULL);
        return self_test_pipelines();
    }
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
        fprintf(stderr, "usage: v380-gst-bridge --url URL --video h264|hevc --audio pcm_s16le|adpcm_ima_wav|alaw|none --audio-block-align N\n");
        return 2;
    }
    if ((strcmp(video_codec, "h264") != 0 && strcmp(video_codec, "hevc") != 0) ||
        (strcmp(audio_codec, "pcm_s16le") != 0 &&
         strcmp(audio_codec, "adpcm_ima_wav") != 0 &&
         strcmp(audio_codec, "alaw") != 0 &&
         strcmp(audio_codec, "none") != 0) ||
        (strcmp(audio_codec, "pcm_s16le") == 0 && audio_block_align != 0) ||
        (strcmp(audio_codec, "adpcm_ima_wav") == 0 &&
         (audio_block_align < 4 || audio_block_align > 8192))) {
        fprintf(stderr, "unsupported codec selection\n");
        return 2;
    }

    /* Application arguments were parsed above; do not pass them to GLib's
       option parser as if they were GStreamer command-line switches. */
    gst_init(NULL, NULL);
    Bridge bridge = { 0 };
    queue_init(&bridge.video_queue, "video", TRUE);
    queue_init(&bridge.audio_queue, "audio", FALSE);
    if (!create_pipeline(&bridge, url, video_codec, audio_codec, audio_block_align, FALSE)) {
        destroy_pipeline(&bridge);
        return 1;
    }
    bridge.loop = g_main_loop_new(NULL, FALSE);
    bridge.loop_thread = g_thread_new("gst-events", event_loop, &bridge);
    while (!g_main_loop_is_running(bridge.loop)) g_usleep(1000);
    bridge.video_feeder = start_feeder(&bridge, &bridge.video_queue);
    if (bridge.audio_src != NULL)
        bridge.audio_feeder = start_feeder(&bridge, &bridge.audio_queue);

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

        Packet *packet = calloc(1, sizeof(*packet));
        if (packet == NULL) {
            fprintf(stderr, "unable to allocate bridge packet\n");
            exit_code = 1;
            break;
        }
        packet->payload = malloc(payload_size);
        if (packet->payload == NULL || !read_exact(stdin, packet->payload, payload_size)) {
            fprintf(stderr, "truncated bridge packet\n");
            packet_free(packet);
            exit_code = 1;
            break;
        }
        packet->type = type;
        packet->flags = flags;
        packet->pts = pts;
        packet->duration = duration;
        packet->payload_size = payload_size;
        packet->arrival_us = g_get_monotonic_time();
        if (type == PACKET_AUDIO && bridge.audio_src == NULL) {
            fprintf(stderr, "audio packet received on video-only pipeline\n");
            packet_free(packet);
            exit_code = 1;
            break;
        }
        if (!queue_push(type == PACKET_VIDEO ? &bridge.video_queue : &bridge.audio_queue, packet) ||
            g_atomic_int_get(&bridge.failed)) {
            exit_code = 1;
            break;
        }
    }

    destroy_pipeline(&bridge);
    return exit_code;
}
