/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <isa-l.h>

//#include "org_apache_orc_isal.h"
#include "LibIsalCodec.h"

static jfieldID ZlibCompressor_clazz;
static jfieldID ZlibCompressor_stream;
static jfieldID ZlibCompressor_finished;

#define ISAL_CODEC_VER   "0.9.0"

JNIEXPORT void JNICALL Java_org_apache_orc_impl_IsalCodec_initIDs(JNIEnv *env, jclass class) {
	printf("%s: enter isal codec. ver %s\n", __FUNCTION__, ISAL_CODEC_VER);
	printf("%s: after load dlsym\n", __FUNCTION__);
	// Initialize the requisite fieldIds
    ZlibCompressor_clazz = (*env)->GetStaticFieldID(env, class, "clazz",
                                                      "Ljava/lang/Class;");
    ZlibCompressor_stream = (*env)->GetFieldID(env, class, "stream", "J");
    ZlibCompressor_finished = (*env)->GetFieldID(env, class, "finished", "Z");
   	printf("%s: leave\n", __FUNCTION__);
}


JNIEXPORT jlong JNICALL Java_org_apache_orc_impl_IsalCodec_deflateinit
  (JNIEnv *env, jclass class, jint level, jint strategy, jbyteArray array, jint offset, jint remain) {
    unsigned char *in = (*env)->GetPrimitiveArrayCritical(env, array, 0);
    unsigned char *level_buf = NULL;
    struct isal_zstream *stream = malloc(sizeof(struct isal_zstream));
    if (!stream) {
		THROW(env, "java/lang/OutOfMemoryError", NULL);
		return (jlong)0;
    }

 //   printf("get level %d, strategy %d\n", level, strategy);
    memset((void*)stream, 0, sizeof(struct isal_zstream));
	// Initialize stream
    isal_deflate_init(stream);
    stream->avail_in = remain;
    stream->next_in = in + offset;
    if(7 == level)
    {
        struct isal_huff_histogram histogram;
        struct isal_hufftables *hufftables_custom = (struct isal_hufftables *)malloc(sizeof(struct isal_hufftables));
        memset(&histogram, 0, sizeof(histogram));
        isal_update_histogram(stream->next_in, stream->avail_in, &histogram);
        isal_create_hufftables(hufftables_custom, &histogram);
        stream->hufftables = hufftables_custom;
        stream->level = 0;
      //  printf("!!!!!!!!use level 0\n");
    }
    else
    {
        level_buf = malloc(ISAL_DEF_LVL1_LARGE);
        if (level_buf == NULL) {
    	    fprintf(stderr, "Can't allocate level buffer memory\n");
    	    return (jint)0;
        }
        stream->level = 1;
        stream->level_buf = level_buf;
        stream->level_buf_size = ISAL_DEF_LVL1_LARGE;
    }

    (*env)->ReleasePrimitiveArrayCritical(env, array, in, 0);

    return JLONG(stream);
}

JNIEXPORT jlong JNICALL Java_org_apache_orc_impl_IsalCodec_inflateinit
  (JNIEnv *env, jclass class, jint level, jint s, jbyteArray array, jint offset, jint remain) {
    unsigned char *in = (*env)->GetPrimitiveArrayCritical(env, array, 0);
    //static const int memLevel = 8; 							// See zconf.h
	  // Create a z_stream
    struct inflate_state *state = malloc(sizeof(struct inflate_state));
    if (!state) {
		THROW(env, "java/lang/OutOfMemoryError", NULL);
		return (jlong)0;
    }
    memset((void*)state, 0, sizeof(struct inflate_state));
    /* allocate deflate state */
	// Initialize stream
    isal_inflate_init(state);
    state->next_in = in + offset;
    state->avail_in = remain;
    (*env)->ReleasePrimitiveArrayCritical(env, array, in, 0);
    return JLONG(state);
}

JNIEXPORT jint JNICALL Java_org_apache_orc_impl_IsalCodec_deflate
  (JNIEnv *env, jobject this, jlong strm, jint flush, jbyteArray array, jint offset, jint remain) {

    int ret;
    //int saved_flag = 0;
   // unsigned int *magic = NULL;
    unsigned char *out = (*env)->GetPrimitiveArrayCritical(env, array, 0);

	// Get members of ZlibCompressor
     struct isal_zstream *stream = ZSTREAM(strm);
    if (!stream) {
		THROW(env, "java/lang/NullPointerException", NULL);
		return (jint)0;
    }
    int have = 0;
	// Re-calibrate the z_stream
  	stream->next_out = out + offset;
    stream->avail_out = remain;
    stream->end_of_stream = 1;
    stream->flush = NO_FLUSH;

	// Compress
	ret = isal_deflate(stream);
	(*env)->ReleasePrimitiveArrayCritical(env, array, out, 0);
    //printf("\nfinish deflate. next_in %p, avail_in %d, total in %d, next_out %p, avail_out %d. total out %d. ret %d. state %d\n",
    //    stream->next_in, stream->avail_in, stream->total_in, stream->next_out, stream->avail_out, stream->total_out,
     //   ret, stream->internal_state.state);
    assert(ret == COMP_OK);  /* state not clobbered */
    switch (ret) {
    case COMP_OK:
        break;
    default:
        (*env)->SetBooleanField(env, this, ZlibCompressor_finished, JNI_TRUE);
        if(NULL != stream->level_buf)
        {
            free(stream->level_buf);
        }
        return ret;
    }

    have = remain - stream->avail_out;
  //  printf("%s: get have %d. remain %d avail_out %d. avail_in %d. total in %d. ret %d, state %d\n",
  //      __FUNCTION__, have, remain, stream->avail_out, stream->avail_in, stream->total_in,
  //      ret, stream->internal_state.state);
    if(/*0 == stream->avail_in && stream->avail_out >= 0 */ZSTATE_END == stream->internal_state.state)
    {
        (*env)->SetBooleanField(env, this, ZlibCompressor_finished, JNI_TRUE);
        if(NULL != stream->level_buf)
        {
            free(stream->level_buf);
        }
    }
    else
    {
   //     THROW(env, "avail_out is invalid, error!!!!!!!!!!!!!!!!!!!", NULL);
    }

  	return have;
}

JNIEXPORT jint JNICALL Java_org_apache_orc_impl_IsalCodec_inflate
  (JNIEnv *env, jobject this, jlong strm, jint flush, jbyteArray array, jint offset, jint remain) {
    int ret;
   // unsigned int *magic = NULL;
    unsigned char *out = (*env)->GetPrimitiveArrayCritical(env, array, 0);
	// Get members of Nzlib
    struct inflate_state *state = ZSTATE(strm);
    if (!state) {
		THROW(env, "java/lang/NullPointerException", NULL);
		return (jint)0;
    }

   // g_output_idx++;

    int have = state->avail_in;
	// Re-calibrate the z_stream
  	state->next_out = out + offset;
    state->avail_out = remain;
   // magic = (unsigned int *)state->next_out;
 // printf("begin inflate. next_in %p, avail_in %d, next_out %p, avail_out %d.\n",
  //      state->next_in, state->avail_in, state->next_out, state->avail_out);
	// Compress
	ret = isal_inflate(state);
	(*env)->ReleasePrimitiveArrayCritical(env, array, out, 0);
  //  printf("finish inflate. next_in %p, avail_in %d, next_out %p, avail_out %d. total_out %d ret %d. block state %d\n",
  //      state->next_in, state->avail_in, state->next_out, state->avail_out, state->total_out, ret, state->block_state);
   // printf("get inflate result %d\n", ret);
    //assert(ret == ISAL_DECOMP_OK);  /* state not clobbered */
    switch (ret) {
    case ISAL_END_INPUT:
    case ISAL_DECOMP_OK:
        break;
    case ISAL_OUT_OVERFLOW:
    default:
      	printf("%s: out buffer overflow. ret %d, in pos %d, out pos %d\n",
      	__FUNCTION__, ret, state->avail_in, state->avail_out);
        return ret;
    }
    have = remain - state->avail_out;
      	//printf("%s: get have %d. ret %d, in pos %d, out pos %d\n",
      	//__FUNCTION__, have, ret, state->avail_in, state->avail_out);

      //	g_total_output += have;
  	return have;
}

JNIEXPORT void JNICALL Java_org_apache_orc_impl_IsalCodec_end
  (JNIEnv *env, jclass class, jlong stream
	) {
	if(NULL !=  ZSTREAM(stream))
	{
	    free(ZSTREAM(stream));
	}

	//printf("total: input %d, output %d, input idx %d, output idx %d\n",
	//g_total_input, g_total_output, g_input_idx, g_output_idx);
}

/**
 * vim: sw=2: ts=2: et:
 */

