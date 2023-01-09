# -*- coding: utf-8 -*-
import datetime
import os
from time import sleep

from pymongo import MongoClient
from requests import Session


def get_connection(username: str, password: str):
    try:
        con = MongoClient(
            f"mongodb+srv://{username}:{password}@scraper-cluster.rnlla8j.mongodb.net/?retryWrites=true&w=majority")
        print()
        print("Checking Database connection: ok")
        return con
    except Exception as e:
        print(f'MongoDBClient Exception: ', e)
        sleep(4)
        print(f'Retrying to connect mongodb client: ')
        return get_connection(username, password)


if __name__ == "__main__":
    auth_token = os.getenv('auth_token')
    platform = os.getenv('platform')

    db_name = 'CosmoFeedLeads'
    conn = get_connection('scraper_mk', 'scraper_mk')
    main_collection_conn = conn[db_name]['GoodCreatorFullDetailsWithoutLogin']

    headers = {
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Authorization': f'Bearer {auth_token}',
        'Host': 'gateway.goodcreator.co',
        'If-None-Match': 'W/"4a26fce885b029f161542289a27a02e2"',
        'Origin': 'https://discovery.goodcreator.co',
        'Referer': 'https://discovery.goodcreator.co/',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'x-bb-channelid': 'ERP',
        'x-bb-clientid': 'nwnvQxFx8iKv5BwzHUKs',
        'x-bb-deviceid': '4ed6c5d7dd867841474398241952ada1',
    }

    sess = Session()

    for x in range(0, 745030, 100):
        error_count = 0
        url = f'https://gateway.goodcreator.co/winkl/get_search_collection_data_with_auth_v2?' \
              f'mfilter=true' \
              f'&initialize=true' \
              f'&followers_lrange=0' \
              f'&followers_urange=307535441' \
              f'&following_lrange=0' \
              f'&following_urange=1714938' \
              f'&ff_lrange=-1114' \
              f'&ff_urange=35913425' \
              f'&eng_lrange=0' \
              f'&eng_urange=19300' \
              f'&l_avg_likes=0' \
              f'&u_avg_likes=6391666' \
              f'&posts_lrange=0' \
              f'&posts_urange=100912' \
              f'&followers=true' \
              f'&following=true' \
              f'&ff=true&posts=true' \
              f'&views_lrange=0' \
              f'&views_urange=23447454901' \
              f'&subscribers_lrange=0' \
              f'&subscribers_urange=101481436' \
              f'&videos_lrange=0' \
              f'&videos_urange=447579&age_urange=99&age_lrange=1&platform_id=1{platform}' \
              f'&offset={str(x)}&limit=100' \
              f'&reset=false&sort=false&l_avg_video_views=0&u_avg_video_views=99999999&order_by=desc&keywords_inclusive=false'
        try:
            print()
            print(f'Offset {x}')
            response = sess.get(url=url, headers=headers)
            if response.ok:
                j_data: dict = response.json()
                print()
                if j_data.get('users') is None:
                    print(j_data)
                    continue
                else:
                    users = j_data.get('users')

                    print(f"Total {len(users)} influence's found.")
                    for index, result in enumerate(users, start=1):
                        print()
                        print(f"{index}: Starts for {result['id']} profile.")
                        result['scrapedDate'] = datetime.datetime.now()
                        if not main_collection_conn.find_one({'id': result["id"]}):
                            main_collection_conn.insert_one(result)
                            print(f'Data saved successfully: {result["id"]}')
                        else:
                            print(f'Data already exists. {result["id"]}')

                        result.clear()

                j_data.clear()
            else:
                print(f'Bad response {response.status_code}')
                print(response.json())

            response.close()
        except Exception as e:
            print("Exception: ", e)
            if error_count > 4:
                break
            error_count += 1
    sess.close()
    conn.close()
