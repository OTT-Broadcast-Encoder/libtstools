/* Exercise ownership transfer to the real asynchronous file writer. */
#include <assert.h>
#include <stdint.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "libltntstools/kl-queue.h"
#include "libltntstools/segmentwriter.h"

static void *writer;
static int64_t expected_size;
static _Atomic(void *) queued_item;
static atomic_int consumed;

void __real_klqueue_push(struct klqueue_s *queue, void *item);
void __real_free(void *item);

void __wrap_free(void *item)
{
	int is_queued_item = item && item == atomic_load(&queued_item);
	__real_free(item);
	if (is_queued_item)
		atomic_store(&consumed, 1);
}

/* Let the consumer finish before the producer returns from enqueue. ASan
 * then detects any access to the object after ownership was transferred. */
void __wrap_klqueue_push(struct klqueue_s *queue, void *item)
{
	atomic_store(&consumed, 0);
	atomic_store(&queued_item, item);
	__real_klqueue_push(queue, item);
	for (int retry = 0; retry < 2000 && !atomic_load(&consumed); retry++)
		usleep(1000);
	assert(atomic_load(&consumed));
	assert(ltntstools_segmentwriter_get_recording_size(writer) == expected_size);
	atomic_store(&queued_item, NULL);
}

int main(int argc, char **argv)
{
	assert(argc == 2 && (!strcmp(argv[1], "copy") || !strcmp(argv[1], "object")));
	int object_mode = !strcmp(argv[1], "object");
	char directory[] = "/tmp/segmentwriter_enqueue_XXXXXX";
	assert(mkdtemp(directory));
	char filename[128], actual[128];
	snprintf(filename, sizeof(filename), "%s/literal.ts", directory);
	assert(ltntstools_segmentwriter_alloc(&writer, filename, "", SEGMENTEDWRITER_SINGLE_FILE) == 0);
	for (int block = 0; block < 256; block++) {
		uint8_t payload[188];
		memset(payload, block, sizeof(payload));
		expected_size += sizeof(payload);
		if (object_mode) {
			void *object;
			uint8_t *destination;
			assert(ltntstools_segmentwriter_object_alloc(writer, sizeof(payload), &object, &destination) == 0);
			memcpy(destination, payload, sizeof(payload));
			assert(ltntstools_segmentwriter_object_write(writer, object) == sizeof(payload));
		} else {
			assert(ltntstools_segmentwriter_write(writer, payload, sizeof(payload)) == sizeof(payload));
		}
	}
	assert(ltntstools_segmentwriter_get_current_filename(writer, actual, sizeof(actual)) == 0);
	assert(!strcmp(actual, filename));
	ltntstools_segmentwriter_free(writer);
	FILE *file = fopen(filename, "rb");
	assert(file);
	for (int block = 0; block < 256; block++) {
		uint8_t payload[188];
		assert(fread(payload, 1, sizeof(payload), file) == sizeof(payload));
		for (size_t i = 0; i < sizeof(payload); i++)
			assert(payload[i] == block);
	}
	assert(fgetc(file) == EOF);
	fclose(file);
	unlink(filename);
	rmdir(directory);
	printf("PASS: %s enqueue ownership, byte counts and literal file contents\n", argv[1]);
	return 0;
}
