/*************************************************************************************************
 * Perl binding
 *************************************************************************************************/


#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <syslog.h>
#include <sys/time.h>
#include <errno.h>

#include <rdkafka.h>

/* global variables */
static char xsversion[10];

typedef struct
  {
    SV *dr_cb;
    SV *stats_cb;
    SV *error_cb;
    SV *log_cb;
    int last_err_code;
    SV *last_err_reason;
  } KcEnv;

/* http://www.perlmonks.org/?node=338857 */

/* We have to bundle up enough of the XS environment to 
   be able to call perl functions from our C callbacks */

static void
do_callback(SV *fun, const int result, const char *msg, const void *data1, const void *data2, const void *data3, const void *data4)
  {
    /*fprintf(stderr, "do_callback called with code=%d, function is %p\n", result, (void *)fun);*/

    dSP;
    ENTER;
    SAVETMPS;
    PUSHMARK(SP);
    XPUSHs(sv_2mortal(newSViv(result)));
    XPUSHs(sv_2mortal(newSVpv(msg,0)));
    if (data1 != NULL)
      XPUSHs(sv_2mortal(newSVpv(data1,0)));
    if (data2 != NULL)
      XPUSHs(sv_2mortal(newSVpv(data2,0)));
    if (data3 != NULL)
      XPUSHs(sv_2mortal(newSVpv(data3,0)));
    if (data4 != NULL)
      XPUSHs(sv_2mortal(newSVpv(data4,0)));
    PUTBACK;
    call_sv(fun, G_VOID | G_DISCARD);
    FREETMPS;
    LEAVE;
  }

static void
dr_cb(rd_kafka_t *rk, void *payload, size_t len, int err, void *opaque, void *msg_opaque)
  {
    char msg_opaque_string[50];
    char valuestring[len+1];
    KcEnv *env_ptr = (KcEnv *)opaque;

    /*fprintf(stderr, "delivery report callback called with error code=%d, opaque=%p, msg_opaque=%p, dr_cb=%p\n", err, opaque, msg_opaque, (void *)env_ptr->dr_cb);*/

    /*if (err != 0)
      fprintf(stderr, "delivery report callback called with error code=%d, error reason: %s\n", err, rd_kafka_err2str(rd_kafka_errno2err(err)));*/

    if (env_ptr == (KcEnv *)NULL) return;
    if (env_ptr->dr_cb == (SV *)NULL) return;

    /*fprintf(stderr, "msg_opaque is %d\n", (int64_t)msg_opaque);*/
    snprintf(msg_opaque_string, 49, "%ld", (int64_t)msg_opaque);
    snprintf(valuestring, len+1, "%s", (char*)payload);
    valuestring[len] = '\0';

    do_callback(env_ptr->dr_cb, err, valuestring, (void *)msg_opaque_string, NULL, NULL, NULL);

  }


static void
dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque)
  {
    char msg_opaque_string[50];
    char key_string[50];
    char valuestring[rkmessage->len+1];
    KcEnv *env_ptr = (KcEnv *)opaque;

    if (env_ptr == (KcEnv *)NULL) return;
    if (env_ptr->dr_cb == (SV *)NULL) return;

    snprintf(msg_opaque_string, 49, "%ld", (int64_t)rkmessage->_private);
    snprintf(valuestring, rkmessage->len+1, "%s", (char*)rkmessage->payload);
    snprintf(key_string, 49, "%s", (char*)rkmessage->key);
    valuestring[rkmessage->len] = '\0';

    do_callback(env_ptr->dr_cb, rkmessage->err, valuestring, (void *)msg_opaque_string, NULL, NULL, NULL);

  }


static void
error_cb(rd_kafka_t *rk, int err, const char *reason, void *opaque)
  {
    const char* errmsg = rd_kafka_err2str(rd_kafka_errno2err(err));
    /*fprintf(stderr, "error callback called with error code=%d, reason: %s\n", err, errmsg);*/

    // using a real callback here may not work because of perl thread reasons, we just set the last error information instead
    if (opaque == NULL) return;
    KcEnv *env_ptr = (KcEnv *)opaque;
    if (env_ptr->error_cb == NULL) return;

    env_ptr->last_err_code = err;
    env_ptr->last_err_reason = newSVpv(reason,0);

    //fprintf(stderr, "Error received in error callback from librdkafka, error code %d (%s), reason: %s", err, errmsg, reason);

    do_callback(env_ptr->error_cb, err, reason, errmsg, NULL, NULL, NULL);

  }


static int
stats_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque)
  {
    char jsonstring[json_len+1];
    /*fprintf(stderr, "stats callback called with statistics: %s\n", json);*/

    if (opaque == NULL) return;
    KcEnv *env_ptr = (KcEnv *)opaque;
    if (env_ptr->stats_cb == NULL) return;
    
    snprintf(jsonstring, json_len+1, "%s", json);

    do_callback(env_ptr->stats_cb, 0, json, NULL, NULL, NULL, NULL);

    return 0;
  }


static void
logger(const rd_kafka_t *rk, int level, const char *fac, const char *buf)
  {
    struct timeval tv;
    char logline[1024];
    gettimeofday(&tv, NULL);
    snprintf(logline, 1023, "%u.%03u RDKAFKA-%i-%s: %s: %s\n",
	(int)tv.tv_sec, (int)(tv.tv_usec / 1000),
	level, fac, rd_kafka_name(rk), buf);

    //KcEnv *env_ptr = (KcEnv *)rd_kafka_opaque(rk);

    //if (env_ptr == NULL) return;
    //if (env_ptr->log_cb == NULL) return;

    // using a real callback here may not work because of perl thread reasons, we just print the logging info
    //do_callback(env_ptr->log_cb, level, logline, fac, NULL, NULL, NULL);
    fprintf(stderr, "%s", logline);
  }


static void
dump_conf(rd_kafka_conf_t *conf)
  {
    const char **arr;
    size_t cnt;
    int i;
    arr = rd_kafka_conf_dump(conf, &cnt);
    for (i=0; i<cnt; i+=2)
      fprintf(stderr, "%s = %s\n", arr[i], arr[i+1]);
  }


static void
dump_topic_conf(rd_kafka_topic_conf_t *conf)
  {
    const char **arr;
    size_t cnt;
    int i;
    arr = rd_kafka_topic_conf_dump(conf, &cnt);
    for (i=0; i<cnt; i+=2)
      fprintf(stderr, "%s = %s\n", arr[i], arr[i+1]);
  }


static void
handle_message(rd_kafka_message_t *rkmessage, void *opaque)
  {
    /*fprintf(stderr, "handle_message %d: %s = %s\n", rkmessage->err, rkmessage->key, rkmessage->payload);*/
    char offsetstring[64];
    char partitionstring[64];
    char valuestring[rkmessage->len+1];
    snprintf(offsetstring, 63, "%ld", rkmessage->offset);
    snprintf(partitionstring, 63, "%d", rkmessage->partition);
    if (rkmessage->payload == NULL) {
      valuestring[0] = '\0';
    }
    else {
      snprintf(valuestring, rkmessage->len+1, "%s", (char*)rkmessage->payload);
      valuestring[rkmessage->len] = '\0';
    }
    if (rkmessage->key == NULL)
      do_callback((SV *)opaque, rkmessage->err, rd_kafka_topic_name(rkmessage->rkt), partitionstring, "", valuestring, offsetstring);
    else
      do_callback((SV *)opaque, rkmessage->err, rd_kafka_topic_name(rkmessage->rkt), partitionstring, rkmessage->key, valuestring, offsetstring);
  }


MODULE = KafkaClient		PACKAGE = KafkaClient
PROTOTYPES: DISABLE


BOOT:
	strncpy(xsversion,"1.00",10);


const char*
VERSION()
PPCODE:
	XPUSHs(sv_2mortal(newSVpv(VERSION, 4)));


void
kc_last_error()
PPCODE:
	const char* errmsg = rd_kafka_err2str(rd_kafka_errno2err(errno));

	XPUSHs(sv_2mortal(newSVpv(errmsg,0)));
	XSRETURN(1);


void
kc_config_new()
PPCODE:
	rd_kafka_conf_t *conf = rd_kafka_conf_new();

	XPUSHs(sv_2mortal(newSViv((IV)conf)));
	XSRETURN(1);


void
kc_config_set(IV ivconf, SV* svname, SV* svvalue)
PPCODE:
	char errstr[512];
	int len = 512;
	int rcode;
	size_t psiz;
	rd_kafka_conf_t *conf = (rd_kafka_conf_t *)ivconf;
	const char* name = SvPV(svname, psiz);
	const char* value = SvPV(svvalue, psiz);
	
	rcode = rd_kafka_conf_set(conf, name, value, errstr, len);

	XPUSHs(sv_2mortal(newSViv(rcode)));
	XSRETURN(1);


void
kc_new(int ptype, IV ivconf)
PPCODE:
	char errstr[512];
	rd_kafka_type_t type;
	KcEnv* env_ptr = malloc(sizeof(KcEnv));
	if (env_ptr == NULL) {
		perror("Cannot allocate memory for env_ptr");
		XPUSHs(&PL_sv_undef);
		XSRETURN(1);
	}
	env_ptr->dr_cb = (SV *)NULL;
	env_ptr->stats_cb = (SV *)NULL;
	env_ptr->error_cb = (SV *)NULL;
	env_ptr->log_cb = (SV *)NULL;
	if (ptype == 1)
		type = RD_KAFKA_PRODUCER;
	else
		type = RD_KAFKA_CONSUMER;

	rd_kafka_conf_t *conf;
	if (ivconf != 0)
		conf = (rd_kafka_conf_t *)ivconf;
	else
		conf = rd_kafka_conf_new();
	rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
	//rd_kafka_conf_set_stats_cb(conf, stats_cb);
	//rd_kafka_conf_set_error_cb(conf, error_cb);
	//rd_kafka_conf_set(conf, "debug", "all", errstr, sizeof(errstr));

	rd_kafka_conf_set_opaque(conf, (void *)env_ptr);

	//dump_conf(conf);

	rd_kafka_t *rk = rd_kafka_new(type, conf, errstr, sizeof(errstr));

	//rd_kafka_set_logger(rk, logger);
	//rd_kafka_set_log_level(rk, LOG_DEBUG);

	if (!rk)
		XPUSHs(&PL_sv_undef);
	else
		XPUSHs(sv_2mortal(newSViv((IV)rk)));
	XSRETURN(1);


void
kc_dump(IV ivrk)
PPCODE:
        rd_kafka_t *rk = (rd_kafka_t*)ivrk;
	rd_kafka_dump(stderr, rk);
        XSRETURN(0);


void
kc_destroy(IV ivrk)
PPCODE:
	rd_kafka_t *rk = (rd_kafka_t*)ivrk;
	int pollcount = 0;
	int outq = 0;

        //fprintf(stderr, "cleaning up kafka handles\n");

	/*while(rd_kafka_outq_len(rk) > 0)
		rd_kafka_poll(rk, 1000);*/

	outq = rd_kafka_outq_len(rk);
	while(outq > 0 && pollcount < 60 && rd_kafka_poll(rk, 1000) != -1) {
		//fprintf(stderr, "polling..\n");
		outq = rd_kafka_outq_len(rk);
		pollcount++;
	}

	//fprintf(stderr, "polled %d times, %d messages remain in outqueue\n", pollcount, outq);

	KcEnv *env_ptr = (KcEnv *)rd_kafka_opaque(rk);
	free(env_ptr);

	//fprintf(stderr, "destroying kafka handles\n");

	rd_kafka_destroy(rk);
	rd_kafka_wait_destroyed(2000);

	XSRETURN(0);


void
kc_add_broker(IV ivrk, SV* svbrokerlist)
PPCODE:
	size_t psiz;
	int rcode;

	rd_kafka_t *rk = (rd_kafka_t*)ivrk;
	const char* brokerlist = SvPV(svbrokerlist, psiz);

	rcode = rd_kafka_brokers_add(rk, brokerlist);

	XPUSHs(sv_2mortal(newSViv(rcode)));
	XSRETURN(1);


void
kc_poll(IV ivrk)
PPCODE:
	rd_kafka_t *rk = (rd_kafka_t*)ivrk;

	rd_kafka_poll(rk, 0);

	XSRETURN(0);


void
kc_get_topics(IV ivrk)
PPCODE:
	int i;
	rd_kafka_t *rk = (rd_kafka_t*)ivrk;
	rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
	int return_num = 1;

	/*XPUSHs(sv_2mortal(newSViv(1)));
	XPUSHs(sv_2mortal(newSVpv("test",0)));
	XSRETURN(2);*/

	const struct rd_kafka_metadata *metadata;

	err = rd_kafka_metadata(rk, 1, NULL, &metadata, 5000);

	if (err == RD_KAFKA_RESP_ERR_NO_ERROR && metadata->topic_cnt > 0) {
		XPUSHs(sv_2mortal(newSViv(metadata->topic_cnt)));
		for (i=0; i < metadata->topic_cnt; i++) {
			XPUSHs(sv_2mortal(newSVpv(metadata->topics[i].topic,0)));
			return_num++;
		}
		rd_kafka_metadata_destroy(metadata);
		XSRETURN(return_num);
	}
	else {
		//fprintf(stderr, "got error while getting metadata: %s", rd_kafka_err2str(err));
		XPUSHs(&PL_sv_undef);
		XSRETURN(1);
	}


void
kc_get_brokers(IV ivrk)
PPCODE:
        int i;
        rd_kafka_t *rk = (rd_kafka_t*)ivrk;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
        int return_num = 1;

        const struct rd_kafka_metadata *metadata;

        err = rd_kafka_metadata(rk, 1, NULL, &metadata, 5000);

        if (err == RD_KAFKA_RESP_ERR_NO_ERROR && metadata->broker_cnt > 0) {
                XPUSHs(sv_2mortal(newSViv(metadata->broker_cnt)));
                for (i=0; i < metadata->broker_cnt; i++) {
                        XPUSHs(sv_2mortal(newSViv(metadata->brokers[i].id)));
                        return_num++;
                }
                rd_kafka_metadata_destroy(metadata);
                XSRETURN(return_num);
        }
        else {
                //fprintf(stderr, "got error while getting metadata: %s", rd_kafka_err2str(err));
                XPUSHs(&PL_sv_undef);
                XSRETURN(1);
        }


void
kc_set_error_cb(IV ivrk, SV *fun)
PPCODE:
	rd_kafka_t *rk = (rd_kafka_t*)ivrk;

	KcEnv *env_ptr = (KcEnv *)rd_kafka_opaque(rk);
	if (!SvOK(fun))
		fun = (SV *)NULL;
	if (env_ptr->error_cb == (SV *)NULL)
		env_ptr->error_cb = newSVsv(fun);
	else
		SvSetSV(env_ptr->error_cb, fun);

	XSRETURN(0);


void
kc_set_stats_cb(IV ivrk, SV *fun)
PPCODE:
	rd_kafka_t *rk = (rd_kafka_t*)ivrk;

	KcEnv *env_ptr = (KcEnv *)rd_kafka_opaque(rk);
	if (!SvOK(fun))
		fun = (SV *)NULL;
	if (env_ptr->stats_cb == (SV *)NULL)
		env_ptr->stats_cb = newSVsv(fun);
	else
		SvSetSV(env_ptr->stats_cb, fun);

	XSRETURN(0);


void
kc_set_dr_cb(IV ivrk, SV *fun)
PPCODE:
	rd_kafka_t *rk = (rd_kafka_t*)ivrk;

	KcEnv *env_ptr = (KcEnv *)rd_kafka_opaque(rk);
	if (!SvOK(fun))
		fun = (SV *)NULL;
	if (env_ptr->dr_cb == (SV *)NULL)
		env_ptr->dr_cb = newSVsv(fun);
	else
		SvSetSV(env_ptr->dr_cb, fun);

	XSRETURN(0);


void
kc_set_log_cb(IV ivrk, SV *fun)
PPCODE:
	rd_kafka_t *rk = (rd_kafka_t*)ivrk;

	KcEnv *env_ptr = (KcEnv *)rd_kafka_opaque(rk);
	if (!SvOK(fun))
		fun = (SV *)NULL;
	if (env_ptr->log_cb == (SV *)NULL)
		env_ptr->log_cb = newSVsv(fun);
	else
		SvSetSV(env_ptr->log_cb, fun);

	XSRETURN(0);


MODULE = KafkaClient		PACKAGE = KafkaClient::Topic
PROTOTYPES: DISABLE


void
kc_topic_config_new()
PPCODE:
	rd_kafka_topic_conf_t *topic_conf = rd_kafka_topic_conf_new();

	XPUSHs(sv_2mortal(newSViv((IV)topic_conf)));
	XSRETURN(1);


void
kc_topic_config_set(IV ivtopicconf, SV* svname, SV* svvalue)
PPCODE:
	char errstr[512];
	int len = 512;
	int rcode;
	size_t psiz;
	rd_kafka_topic_conf_t *topic_conf = (rd_kafka_topic_conf_t *)ivtopicconf;
	const char* name = SvPV(svname, psiz);
	const char* value = SvPV(svvalue, psiz);
	
	rcode = rd_kafka_topic_conf_set(topic_conf, name, value, errstr, len);

	XPUSHs(sv_2mortal(newSViv(rcode)));
	XSRETURN(1);


void
kc_topic_new(IV ivrk, SV* svtopic, IV ivtopicconf)
PPCODE:
	rd_kafka_topic_t *rkt;
	size_t psiz;

	rd_kafka_t *rk = (rd_kafka_t*)ivrk;
	const char* topic = SvPV(svtopic, psiz);
	rd_kafka_topic_conf_t *topic_conf;
	if (ivtopicconf != 0)
		topic_conf = (rd_kafka_topic_conf_t*)ivtopicconf;
	else
		topic_conf = rd_kafka_topic_conf_new();

	//dump_topic_conf(topic_conf);

	rkt = rd_kafka_topic_new(rk, topic, topic_conf);

	if (!rkt)
		XPUSHs(&PL_sv_undef);
	else
		XPUSHs(sv_2mortal(newSViv((IV)rkt)));
	XSRETURN(1);


void
kc_topic_destroy(IV ivrkt)
PPCODE:
	rd_kafka_topic_t *rkt = (rd_kafka_topic_t*)ivrkt;

	//fprintf(stderr, "Destroying topic\n");

	rd_kafka_topic_destroy(rkt);

	XSRETURN(0);


void
kc_create_queue(IV ivrk)
PPCODE:
	rd_kafka_queue_t *rkqu;

	rd_kafka_t *rk = (rd_kafka_t*)ivrk;
	rkqu = rd_kafka_queue_new(rk);

	if (!rkqu)
		XPUSHs(&PL_sv_undef);
	else
		XPUSHs(sv_2mortal(newSViv((IV)rkqu)));
	XSRETURN(1);


void
kc_destroy_queue(IV ivrkqu)
PPCODE:
	rd_kafka_queue_t *rkqu = (rd_kafka_queue_t*)ivrkqu;

	rd_kafka_queue_destroy(rkqu);

	XSRETURN(0);


void
kc_topic_produce(IV ivrkt, SV* svbuf, SV* svpartition, SV *svopaque)
PPCODE:
	int rcode;
	size_t len;
	int32_t partition;

	rd_kafka_topic_t *rkt = (rd_kafka_topic_t*)ivrkt;
	const char* buf = SvPV(svbuf, len);
	int64_t opaque = SvIV(svopaque);
	/*fprintf(stderr, "msg_opaque is %d\n", opaque);*/

	if (!SvOK(svpartition))
		partition = RD_KAFKA_PARTITION_UA;
	else
		partition = SvIV(svpartition);

	rcode = rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY, (void *)buf, len, NULL, 0, (void *)opaque);

	/*fprintf(stderr, "produce called with return code=%d, msg was %s\n", rcode, buf);*/

	//fprintf(stderr, "%% Sent %zd bytes to topic %s partition %i\n", len, rd_kafka_topic_name(rkt), partition);

	XPUSHs(sv_2mortal(newSViv(rcode)));
	XSRETURN(1);


void
kc_topic_produce_with_key(IV ivrkt, SV* svbuf, SV* svpartition, SV *svopaque, SV *svkey)
PPCODE:
        int rcode;
        size_t len;
	size_t keylen;
        int32_t partition;

        rd_kafka_topic_t *rkt = (rd_kafka_topic_t*)ivrkt;
        const char* buf = SvPV(svbuf, len);
	const char* key = SvPV(svkey, keylen);
        int64_t opaque = SvIV(svopaque);

        if (!SvOK(svpartition))
                partition = RD_KAFKA_PARTITION_UA;
        else
                partition = SvIV(svpartition);

        rcode = rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY, (void *)buf, len, (void *)key, keylen, (void *)opaque);

        XPUSHs(sv_2mortal(newSViv(rcode)));
        XSRETURN(1);


void
kc_topic_consume_start(IV ivrkt, SV *svpartition, SV *svstartoffset)
PPCODE:
	int rcode;
	int32_t partition;
	int32_t startoffset;

	rd_kafka_topic_t *rkt = (rd_kafka_topic_t*)ivrkt;
	startoffset = SvIV(svstartoffset);
	
	if (!SvOK(svpartition))
		partition = RD_KAFKA_PARTITION_UA;
	else
		partition = SvIV(svpartition);

	rcode = rd_kafka_consume_start(rkt, partition, startoffset);

	XPUSHs(sv_2mortal(newSViv(rcode)));
	XSRETURN(1);


void
kc_topic_consume_stop(IV ivrkt, SV *svpartition)
PPCODE:
	int rcode;
	int32_t partition;
	//rd_kafka_toppar_t *rktp;

	/*fprintf(stderr, "Stopping consumer\n");*/

	rd_kafka_topic_t *rkt = (rd_kafka_topic_t*)ivrkt;

	if (!SvOK(svpartition))
		partition = RD_KAFKA_PARTITION_UA;
	else
		partition = SvIV(svpartition);

	/*if (partition == 0) {
		rktp = rkt->rkt_p[partition];
		fprintf(stderr, "Stopping consumer, topic has offset %d and eof offset %d\n", rktp->rktp_stored_offset, rktp->rktp_eof_offset);
	}*/

	rcode = rd_kafka_consume_stop(rkt, partition);

        /*if (partition == 0) {
                rktp = rkt->rkt_p[partition];
                fprintf(stderr, "Stopping consumer, topic has offset %d and eof offset %d\n", rktp->rktp_stored_offset, rktp->rktp_eof_offset);
        }*/

	XPUSHs(sv_2mortal(newSViv(rcode)));
	XSRETURN(1);


void
kc_topic_consume(IV ivrkt, SV *svpartition, SV *fun, int timeout)
PPCODE:
	int rcode;
	int32_t partition;
	rd_kafka_message_t *rkmessage;

	rd_kafka_topic_t *rkt = (rd_kafka_topic_t*)ivrkt;
	
	if (!SvOK(svpartition))
		partition = RD_KAFKA_PARTITION_UA;
	else
		partition = SvIV(svpartition);

	/*rkmessage = rd_kafka_consume(rkt, partition, timeout);

	rcode = rkmessage->err;

	if (!rcode) {
		handle_message(rkmessage, fun);
	}

	rd_kafka_message_destroy(rkmessage);*/

	rcode = rd_kafka_consume_callback(rkt, partition, timeout, handle_message, (void *)fun);

	XPUSHs(sv_2mortal(newSViv(rcode)));
	XSRETURN(1);


void
kc_topic_consume_start_queue(IV ivrkt, SV *svpartition, SV *svstartoffset, IV ivrkqu)
PPCODE:
        int rcode;
        int32_t partition;
        int32_t startoffset;

        rd_kafka_topic_t *rkt = (rd_kafka_topic_t*)ivrkt;
	rd_kafka_queue_t *rkqu = (rd_kafka_queue_t*)ivrkqu;

        startoffset = SvIV(svstartoffset);

        if (!SvOK(svpartition))
                partition = RD_KAFKA_PARTITION_UA;
        else
                partition = SvIV(svpartition);

	rcode = rd_kafka_consume_start_queue(rkt, partition, startoffset, rkqu);

        XPUSHs(sv_2mortal(newSViv(rcode)));
        XSRETURN(1);


void
kc_topic_consume_queue(IV ivrkqu, SV *fun, int timeout)
PPCODE:
        int rcode;

	rd_kafka_queue_t *rkqu = (rd_kafka_queue_t*)ivrkqu;

        rcode = rd_kafka_consume_callback_queue(rkqu, timeout, handle_message, (void *)fun);

        XPUSHs(sv_2mortal(newSViv(rcode)));
        XSRETURN(1);


void
kc_get_partitions(IV ivrk, IV ivrkt)
PPCODE:
	int i;
	rd_kafka_t *rk = (rd_kafka_t*)ivrk;
	rd_kafka_topic_t *rkt = (rd_kafka_topic_t*)ivrkt;
	rd_kafka_resp_err_t err;
	int return_num = 1;

	/*XPUSHs(sv_2mortal(newSViv(2)));
	XPUSHs(sv_2mortal(newSViv(0)));
	XPUSHs(sv_2mortal(newSViv(1)));
	XSRETURN(3);*/

	const struct rd_kafka_metadata *metadata;

	err = rd_kafka_metadata(rk, 0, rkt, &metadata, 5000);

	if (!err && metadata->topic_cnt > 0 && !metadata->topics[0].err && metadata->topics[0].partition_cnt > 0) {
		XPUSHs(sv_2mortal(newSViv(metadata->topics[0].partition_cnt)));
		for (i=0; i < metadata->topics[0].partition_cnt; i++) {
			XPUSHs(sv_2mortal(newSViv(metadata->topics[0].partitions[i].id)));
			return_num++;
		}
		rd_kafka_metadata_destroy(metadata);
		XSRETURN(return_num);
	}
	else {
		//fprintf(stderr, "got error while getting metadata for topic %s: %s", rd_kafka_topic_name(rkt), rd_kafka_err2str(err));
		XPUSHs(&PL_sv_undef);
		XSRETURN(1);
	}


## END OF FILE
