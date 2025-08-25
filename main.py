#!/usr/bin/env python3
# kafka_crash_recovery_demo.py
import argparse, time, threading, json, os, random, sys

# ---------- 공통: "작업 1건"은 sleep으로 흉내 ----------
def do_work_ms(ms: int):
    time.sleep(ms / 1000.0)

# ---------- [A] SINGLE: 크래시 데모 (중간 종료 → 진행도 유실) ----------
def crash_single(n, work_ms, crash_after):
    print(f"[single-crash] jobs={n}, work_ms={work_ms}ms, crash_after={crash_after}s")
    t0 = time.time()
    processed = 0
    try:
        for i in range(n):
            do_work_ms(work_ms)
            processed += 1
            if (crash_after is not None) and (time.time() - t0 >= crash_after):
                print(f"[single-crash] CRASH NOW at processed={processed}")
                os._exit(1)  # 강제 종료(진행도 저장 X)
        print(f"[single-crash] finished. processed={processed}/{n}")
    except KeyboardInterrupt:
        print("[single-crash] interrupted")

# ---------- [B] KAFKA: 크래시 데모 (중간 종료 후 재시작 → 이어서 처리) ----------
def kafka_crash_produce(n, bootstrap, topic, partitions=2):
    try:
        from kafka import KafkaProducer # 실행, 설치가 안돼있으면 exception 발생
        from kafka.admin import KafkaAdminClient, NewTopic # 실행, 설치가 안돼있으면 exception 발생
        from kafka.errors import TopicAlreadyExistsError # 실행, 설치가 안돼있으면 exception 발생
    except Exception:
        sys.exit("pip install kafka-python 해주세요.")
    
    # 토픽 생성
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="crash-demo") # Kafka 서버(localhost:9092)에 관리자로 연결
        try:
            admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=1)])
            print(f"[kafka] created topic '{topic}' partitions={partitions}") # 파티션은 기본 2로 설정
        except TopicAlreadyExistsError:
            print(f"[kafka] topic '{topic}' exists. (ok)")
        finally:
            admin.close()
    except Exception as e:
        print(f"[kafka] admin skipped: {e}") # Kafka 서버가 꺼져있거나, 권한 문제, 네트워크 연결 문제 등
    
    # 전송(균등 분배)
    # 변환 방법: 파이썬 데이터 -> JSON 문자열 -> 바이트 데이터
    prod = KafkaProducer(bootstrap_servers=bootstrap, value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    for i in range(n):
        prod.send(topic, {"i": i}, partition=i % partitions) # 파티션 0, 1, 2, 3에 분배
    prod.flush()
    print(f"[kafka] produced {n} messages to '{topic}'")

def kafka_crash_consume(bootstrap, topic, group_id, work_ms, crash_after=None):
    from kafka import KafkaConsumer
    import time, os

    print(f"[kafka-crash] consuming topic={topic}, group={group_id}, work_ms={work_ms}ms, crash_after={crash_after}")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,      # 처리 후에만 커밋
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        max_poll_records=10,           # ★ 한 번에 배치 10개 가져옴
    )

    t0 = time.time()
    processed = 0
    idle_since = time.time()

    try:
        while True:
            # 메시지 가져오기
            batch = consumer.poll(timeout_ms=200) # 200ms 대기 후, 없으면 빈 배치 반환
            if not batch:
                # 일정 시간 동안 새 레코드가 없으면 종료
                if time.time() - idle_since > 3:
                    print(f"[kafka-crash] idle exit. processed={processed}")
                    break
                continue

            idle_since = time.time()

            for tp, records in batch.items():
                for rec in records:
                    # 1건 처리
                    do_work_ms(work_ms)
                    processed += 1

                    # ★ 레코드 '중간'에도 크래시 검사
                    if crash_after and (time.time() - t0) >= crash_after:
                        print(f"[kafka-crash] CRASH NOW at processed={processed} "
                              f"(current batch NOT committed)")
                        os._exit(1)  # 커밋 전에 죽어서, 현재 배치는 재시작 시 다시 읽힘

            # 배치 단위 커밋 (여기까지는 처리 완료)
            consumer.commit()

    finally:
        consumer.close()
        print(f"[kafka-crash] done processed={processed}")

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="Crash Recovery Demo: Single vs Kafka")
    sub = ap.add_subparsers(dest="cmd", required=True)

    # A) single crash
    cs = sub.add_parser("crash-single", help="Single 모드 크래시 실험 (복구 불가)")
    cs.add_argument("-n","--jobs", type=int, default=100, help="총 작업 개수")
    cs.add_argument("-t","--work-ms", type=int, default=100, help="작업 1개당 소요 시간(ms)")
    cs.add_argument("-C","--crash-after", type=float, default=None, help="몇 초 후 크래시할지")

    # B) kafka crash - produce
    kpp = sub.add_parser("kafka-produce", help="Kafka 메시지 생성")
    kpp.add_argument("-n","--jobs", type=int, default=100, help="총 메시지 개수")
    kpp.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP","localhost:9092"))
    kpp.add_argument("--topic", default="crash_demo", help="Kafka 토픽명")
    kpp.add_argument("--partitions", type=int, default=2, help="파티션 개수")

    # C) kafka crash - consume
    kpc = sub.add_parser("kafka-consume", help="Kafka 메시지 소비 (복구 가능)")
    kpc.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP","localhost:9092"))
    kpc.add_argument("--topic", default="crash_demo", help="Kafka 토픽명")
    kpc.add_argument("--group-id", default="crash-demo-group", help="Consumer Group ID")
    kpc.add_argument("-t","--work-ms", type=int, default=100, help="작업 1개당 소요 시간(ms)")
    kpc.add_argument("-C","--crash-after", type=float, default=None, help="몇 초 후 크래시할지")

    args = ap.parse_args()
    if args.cmd == "crash-single":
        crash_single(args.jobs, args.work_ms, args.crash_after)
    elif args.cmd == "kafka-produce":
        kafka_crash_produce(args.jobs, args.bootstrap, args.topic, args.partitions)
    elif args.cmd == "kafka-consume":
        kafka_crash_consume(args.bootstrap, args.topic, args.group_id, args.work_ms, args.crash_after)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()