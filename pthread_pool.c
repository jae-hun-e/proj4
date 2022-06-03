/*
 * Copyright 2022. Heekuck Oh, all rights reserved
 * 이 프로그램은 한양대학교 ERICA 소프트웨어학부 재학생을 위한 교육용으로 제작되었습니다.
 */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "pthread_pool.h"

/*
 * 풀에 있는 일꾼(일벌) 스레드가 수행할 함수이다.
 * FIFO 대기열에서 기다리고 있는 작업을 하나씩 꺼내서 실행한다.
 */
static void *worker(void *param)
{
    task_t task;
    pthread_pool_t* pool = param;

    while(pool->running) {
        pthread_mutex_lock(&pool->mutex);
        while(is_empty(pool) && pool->running) {
            pthread_cond_wait(&pool->full, &pool->mutex);
        }

        if (!pool->running) {
            pthread_mutex_unlock(&pool->mutex);
            pthread_exit(0);
        }

        task = dequeue(pool);

        pthread_cond_signal(&pool->empty);
        pthread_mutex_unlock(&pool->mutex);
        task.function(task.param);
    }
    pthread_exit(0);
}

/*
 * 스레드풀을 초기화한다. 성공하면 POOL_SUCCESS를, 실패하면 POOL_FAIL을 리턴한다.
 * bee_size는 일꾼(일벌) 스레드의 갯수이고, queue_size는 작업 대기열의 크기이다.
 * 대기열의 크기 queue_size가 최소한 일꾼의 수 bee_size보다 크거나 같게 만든다.
 */
int pthread_pool_init(pthread_pool_t *pool, size_t bee_size, size_t queue_size)
{
    if (bee_size > POOL_MAXBSIZE || queue_size > POOL_MAXQSIZE) {
        return POOL_FAIL;
    }
    if (queue_size < bee_size) {
        queue_size = bee_size;
    }

    // bee size 만큼 Thread Pool 초기화
    pool->running = true;

    // 원형 큐 생성
    pool->q_size = queue_size;
    pool->q_front = 0;
    pool->q_len = 0;

    // MutexLock 초기화
    pthread_mutex_init(&pool->mutex, NULL);
    pthread_cond_init(&pool->full, NULL);
    pthread_cond_init(&pool->empty, NULL);

    pool->bee_size = bee_size;

    pool->bee = (pthread_t*)malloc(sizeof(pthread_t) * bee_size);

    pthread_mutex_lock(&pool->mutex);

    for (int i = 0; i < bee_size; i++) {
        if (pthread_create(&pool->bee[i], NULL, worker, pool) != 0) {
            return POOL_FAIL;
        }
    }
    pthread_mutex_unlock(&pool->mutex);

    return POOL_SUCCESS;
}

/*
 * 스레드풀에서 실행시킬 함수와 인자의 주소를 넘겨주며 작업을 요청한다.
 * 스레드풀의 대기열이 꽉 찬 상황에서 flag이 POOL_NOWAIT이면 즉시 POOL_FULL을 리턴한다.
 * POOL_WAIT이면 대기열에 빈 자리가 나올 때까지 기다렸다가 넣고 나온다.
 * 작업 요청이 성공하면 POOL_SUCCESS를 리턴한다.
 */
int pthread_pool_submit(pthread_pool_t *pool, void (*f)(void *p), void *p, int flag)
{
    // 여기를 완성하세요
    pthread_mutex_lock(&pool->mutex);
    if (!pool->running) {
        pthread_mutex_unlock(&pool->mutex);
        pthread_exit(0);
    }
    task_t task;
    task.function = f;
    task.param = p;

    if (is_full(pool)) {
        if (flag == POOL_NOWAIT) {
            pthread_mutex_unlock(&pool->mutex);
            return POOL_FULL;
        }
        else if (flag == POOL_WAIT) {
            while(is_full(pool) && pool->running) {
                pthread_cond_wait(&(pool->empty), &(pool->mutex));
            }
            if (!pool->running) {
                pthread_mutex_unlock(&pool->mutex);
                pthread_exit(0);
            }
        }
    }

    enqueue(pool, task);

    pthread_cond_signal(&pool->full);
    pthread_mutex_unlock(&(pool->mutex));
    return POOL_SUCCESS;
}

/*
 * 모든 일꾼 스레드를 종료하고 스레드풀에 할당된 자원을 모두 제거(반납)한다.
 * 락을 소유한 스레드를 중간에 철회하면 교착상태가 발생할 수 있으므로 주의한다.
 * 부모 스레드는 종료된 일꾼 스레드와 조인한 후에 할당된 메모리를 반납한다.
 * 종료가 완료되면 POOL_SUCCESS를 리턴한다.
 */
int pthread_pool_shutdown(pthread_pool_t *pool)
{
//    if (pool == NULL) return POOL_SUCCESS;
//    pthread_mutex_lock(&pool->mutex);
    pool->running = false;

    // 큐 할당 제거하기
//    pool->q_len = 0;

    // Thread 종료까지 대기 하기
//    while(pool->bee_size > 0) {
//        int cur_size = pool->bee_size;
//        pthread_cond_broadcast(&pool->empty);
//        pthread_mutex_unlock(&pool->mutex);
//        for(int i = 0; i < cur_size; i++) {
//            if (pthread_join(pool->bee[i], NULL) != 0) {
//                return POOL_FAIL;
//            }
//        }
//        pthread_mutex_lock(&pool->mutex);
//    }

    pthread_cond_broadcast(&pool->empty);
    pthread_cond_broadcast(&pool->full);
    for (int i = 0 ; i < pool->bee_size; i++) {
        pthread_join(pool->bee[i], NULL);
    }
//    pthread_mutex_unlock(&pool->mutex);

//    pthread_cond_destroy(&pool->full);
//    pthread_cond_destroy(&pool->empty);
//    pthread_mutex_destroy(&pool->mutex);
    free(pool->bee);
    free(pool->q);
    return POOL_SUCCESS;
}

bool is_empty(pthread_pool_t *pool) {
    return pool->q_len == 0;
}

bool is_full(pthread_pool_t *pool) {
    return pool->q_len == pool->q_size;
}

void enqueue(pthread_pool_t *pool, task_t task) {
    int rear;
    rear = (pool->q_front + pool->q_len++) % pool->q_size;
    pool->q[rear].function = task.function;
    pool->q[rear].param = task.param;
}

task_t dequeue(pthread_pool_t *pool) {
    task_t task;
    task.function = pool->q[pool->q_front].function;
    task.param = pool->q[pool->q_front].param;

    pool->q_front = (pool->q_front + 1) % pool->q_size;
    pool->q_len--;
    return task;
}