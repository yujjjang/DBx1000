#include "msg_queue.h"
#include "mem_alloc.h"
#include "query.h"
#include "txn_pool.h"

//#define CONCURRENT_QUEUE
//#define CONCURRENT_QUEUE_RR

void MessageQueue::init() {
  cnt = 0;
  idx = 0;
  head = NULL;
  tail = NULL;
  pthread_mutex_init(&mtx,NULL);
}

void MessageQueue::enqueue(base_query * qry,RemReqType type,uint64_t dest, uint64_t tid) {
  msg_entry_t entry;
  msg_pool.get(entry);
  if(type == RFIN) {
    base_query * qry2;
    qry_pool.get(qry2);
    qry2->pid = qry->pid;
    qry2->rc = qry->rc;
    qry2->txn_id = qry->txn_id;
    qry2->batch_id = qry->batch_id;
    qry2->ro = qry->ro;
    qry2->rtype = qry->rtype;
    entry->qry = qry2;
  } else {
    entry->qry = qry;
  }
  entry->dest = dest;
  entry->type = type;
  entry->next  = NULL;
  entry->prev  = NULL;
  entry->tid = tid;
  entry->starttime = get_sys_clock();
  ATOM_ADD(cnt,1);
  //printf("enq %ld: %f\n",tid,(float)(entry->starttime - g_starttime)/ BILLION);
#ifdef CONCURRENT_QUEUE
  mq.enqueue(entry);
#else
  pthread_mutex_lock(&mtx);
  LIST_PUT_TAIL(head,tail,entry);
  pthread_mutex_unlock(&mtx);
#endif


}
void MessageQueue::enqueue(base_query * qry,RemReqType type,uint64_t dest) {
  msg_entry_t entry;
  msg_pool.get(entry);
  if(type == RFIN) {
    base_query * qry2;
    qry_pool.get(qry2);
    qry2->pid = qry->pid;
    qry2->rc = qry->rc;
    qry2->txn_id = qry->txn_id;
    qry2->batch_id = qry->batch_id;
    qry2->ro = qry->ro;
    qry2->rtype = qry->rtype;
    entry->qry = qry2;
  } else {
    entry->qry = qry;
  }
  entry->dest = dest;
  entry->type = type;
  entry->next  = NULL;
  entry->prev  = NULL;
  entry->tid = UINT64_MAX;
  entry->starttime = get_sys_clock();
  ATOM_ADD(cnt,1);

#ifdef CONCURRENT_QUEUE
  mq.enqueue(entry);
#else
  pthread_mutex_lock(&mtx);
  LIST_PUT_TAIL(head,tail,entry);
  pthread_mutex_unlock(&mtx);
#endif


}

uint64_t MessageQueue::dequeue(base_query *& qry, RemReqType & type, uint64_t & dest, uint64_t & tid) {
  msg_entry_t entry;
  uint64_t time;
#ifdef CONCURRENT_QUEUE

#ifdef CONCURRENT_QUEUE_RR
  bool r = mq.try_dequeue_rr(idx % g_thread_cnt,entry);
#else
  bool r = mq.try_dequeue(idx % g_thread_cnt,entry);
#endif
  ATOM_ADD(idx,1);

#else
  pthread_mutex_lock(&mtx);
  LIST_GET_HEAD(head,tail,entry);
  pthread_mutex_unlock(&mtx);
  bool r = entry != NULL;
#endif
  if(r) {
    ATOM_SUB(cnt,1);
    qry = entry->qry;
    type = entry->type;
    dest = entry->dest;
    time = entry->starttime;
    tid = entry->tid;
    msg_pool.put(entry);
  } else {
    qry = NULL;
    type = NO_MSG;
    dest = UINT64_MAX;
    time = 0;
    tid = UINT64_MAX;
  }
  return time;
}
