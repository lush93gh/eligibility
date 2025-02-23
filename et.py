import pandas as pd
from datetime import datetime
from collections import defaultdict
from heapq import heappop, heappush, heapify

"""
將 dateTime string 轉成時間物件
"""
def time_map(x):
    if x.__contains__("."):
        return datetime.strptime(x.split(".")[0], "%Y-%m-%d %H:%M:%S")
    else: 
        return datetime.strptime(x.split(" UTC")[0], "%Y-%m-%d %H:%M:%S")

"""
Decay Factor over time_steps
"""
def decay_factor(gamma, time_steps):
    return pow(gamma, time_steps)

"""
遞增函式 (+1)
"""
def map_counter(map, e):
    map[e] = map.get(e, 0) + 1


"""
    給定 "event_name" 和 "timestamp"，
    紀錄這位 user 自己所經歷過的各個 event 所對應的 et_score

    schema: {
                "event_name": et_score
            }
"""
def eligibility_traces(gamma, events):
    # t0 is initialized to infinte
    t0 = datetime.max
    # 這位 user 前一個 event 和 t0 的時間差
    prev_diff = 0

    user_et = defaultdict(int)

    for event_name, t in events:
        # 紀錄 t0，也就是 user 的第一個 event
        if t < t0:
            t0 = t
        # 計算此 event 和 t0 的時間差，這邊以分鐘計算
        time_diff = (t - t0).seconds // 60

        # 如果時間差和前一個 event 一樣，就不用遞減分數。 因為同一個時間只會對先前的 events 遞減一次
        # 例如：兩個 event 在同一分鐘發生，在他們之前發生過的 events 只需要處理一次遞減
        # 如果時間差和前一個 event 不同，代表時間已經往前推進，需要對之前的 events 遞減分數
        if time_diff > prev_diff:
            # 從 t_prev 到現在的所有 events 都要做遞減。
            discount_factor = decay_factor(gamma, time_diff - prev_diff)
            user_et = {k: v * discount_factor for k, v in user_et.items()}
            # 紀錄這個 event 的時間差
            prev_diff = time_diff
        # 對此 event +1，因為這個時間有踩到這個 event
        map_counter(user_et, event_name)

    # 對該位 user 的所有 events 按照 et_score 從大到小排序
    # 這個 map 是 (event_name 對應 et_score)
    user_et = dict(sorted(user_et.items(), key=lambda item: -float(item[1])))
        
    return user_et


"""
Loop 每位 user 的 et_score map，
每位 user 都投自己 et_score 最高的那個 event 一票 (Top1)
"""
def vote(user_ets):
    # 這個 map 紀錄 (event_name 對應的 票數)
    top_ets = defaultdict(int)

    # Loop 每位 user 的 et_score map，每位 user 都投自己 et_score 最高的那個 event 一票 (Top1)
    for user_et in user_ets:
        # print(user_et)
         # 略過某些 events
        filtered_et = filter(lambda event_name: event_name not in ['subscribe_success', 'tag_add'], user_et)
        for k in filtered_et:
            # 對該 event 進行 +1 投票
            map_counter(top_ets, k)
            # 因為一人一票，所以投完就跳出
            break

    # 按照票數，從大到小排序
    top_ets = dict(sorted(top_ets.items(), key=lambda item: -item[1]))
    # 印結果
    print(top_ets)

    return top_ets


"""
    回傳一個資料結構，每個 element 代表的是：一個 user 的所有 (event_name 對應的 et_score)

    schema: [
        {
            "event_name": et_score
        }, ...
    ]
"""
def calculate_eligibility_traces(user_group, output_path, gamma):
    user_ets = []
    user_last_events = []
        
    # Loop for user, 每位 user 分開算他們自己所經歷過的各個 event 所對應的 et_score
    for name, group in user_group:
        events = list(zip(group.event_name, group.event_timestamp))
        user_et = eligibility_traces(gamma, events)
        # print(user_et)
        # 將該位 user 的 et_score map 加到 list 中
        user_ets.append(user_et)
        user_last_events.append(events[-5:])

    #print(len(user_ets))
    
    # 每位 user 都投自己 et_score 最高的那個 event 一票 (Top1)
    top_ets = vote(user_ets)
    
    result = pd.DataFrame(data=top_ets, index=["votes"])
    # 存結果
    result.to_csv(output_path)

    return top_ets


def accuracy(top_ets, user_last_events):
    number_matches = 0
    for et in top_ets:
        if et in user_last_events:
            number_matches += 1
    return number_matches / len(top_ets)


before = pd.read_csv("before_sub_events.csv")
before = before[["user_pseudo_id", "event_name", "event_timestamp"]]
# 從 string 轉成時間物件
before['event_timestamp'] = before['event_timestamp'].apply(time_map)
user_group = before.groupby('user_pseudo_id')
calculate_eligibility_traces(user_group = user_group, output_path = 'eligibility_traces_rank.csv', gamma=  0.99)
