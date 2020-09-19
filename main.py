from urllib.request import urlopen, Request
from urllib.parse import quote_plus
from bs4 import BeautifulSoup
from selenium import webdriver
from multiprocessing import Process, Lock
import time
import warnings
import pandas as pd
import os


def is_include_tags(hashtags, tags):
    num_of_tags = len(tags)
    cnt = 0
    for elem in tags:
        if elem in hashtags:
            cnt += 1
    if cnt == num_of_tags:
        return True
    else:
        return False


def parsing(lock, process_num, process_idx, reallink, tags):
    # 분할된 데이터 및 데이터 배치 사이즈 나누기
    num_of_data = len(reallink)
    batch_size = 100
    if num_of_data % batch_size == 0:
        num_batch = num_of_data // batch_size
    else:
        num_batch = num_of_data // batch_size + 1
    for batch in range(num_batch):
        if not batch % process_num == process_idx:
            continue
        batch += 1
        print(f'process {process_idx+1}is running, batch number is {batch}')
        csvtext = []
        num_of_saved = 0
        print(f'{num_batch}개 중 {batch} 번째 batch 입니다.')
        start = (batch - 1) * batch_size
        if batch == num_batch:
            end = num_of_data
        else:
            end = batch * batch_size

        for i in range(start, end):
            hashtags = []
            req = Request("https://www.instagram.com/p" + reallink[i], headers={'User-Agent': 'Mozila/5.0'})
            webpage = urlopen(req).read()
            soup = BeautifulSoup(webpage, 'lxml', from_encoding='utf-8')
            soup1 = soup.find('meta', attrs={'property': "og:description"})

            # 본문 내용
            reallink1 = soup1['content']
            reallink1 = reallink1[reallink1.find("@") + 1:reallink1.find(")")]
            # reallink1 = reallink1[:20]
            if reallink1 == '':
                reallink1 = "Null"

            # 태그 내용
            for reallink2 in soup.find_all('meta', attrs={'property': "instapp:hashtags"}):
                hashtag = reallink2['content'].rstrip(',')
                hashtags.append(hashtag)

            # 파싱한 태그 내용이 검색어에 모두 포함돼있으면 csv로 저장함
            if is_include_tags(hashtags, tags):
                csvtext.append([])
                csvtext[num_of_saved].append(reallink1)
                csvtext[num_of_saved].extend(hashtags)
                num_of_saved += 1
        data = pd.DataFrame(csvtext)

        lock.acquire()
        if not os.path.exists(f'{tags[0]}.csv'):
            data.to_csv(f'{tags[0]}.csv', index=False, mode='w', encoding='utf-8-sig')
        else:
            data.to_csv(f'{tags[0]}.csv', index=False, mode='a', encoding='utf-8-sig', header=False)
        lock.release()
    print(f'process {process_idx + 1}is terminated')


def main():
    warnings.filterwarnings(action='ignore')

    base_url = "https://www.instagram.com/explore/tags/"
    tags = input('검색할 태그를 입력하세요 : ')
    tags = tags.replace(' ', '')
    tags = tags.split(',')

    url = base_url + quote_plus(tags[0])

    print("Chrome Driver를 실행합니다.")
    driver = webdriver.Chrome(executable_path="./chromedriver.exe")
    driver.get(url)
    time.sleep(10)

    # # 로그인 하기
    # login_section = '//*[@id="react-root"]/section/nav/div[2]/div/div/div[3]/div/span/a[1]/button'
    # driver.find_element_by_xpath(login_section).click()
    # time.sleep(2)
    #
    # elem_login = driver.find_element_by_name("username")
    # elem_login.clear()
    # elem_login.send_keys('instagram_id')
    #
    # elem_login = driver.find_element_by_name('password')
    # elem_login.clear()
    # elem_login.send_keys('instagram_password')
    #
    # time.sleep(3)
    #
    # xpath = '//*[@id="react-root"]/section/main/div/article/div/div[1]/div/form/div/div[3]/button'
    # driver.find_element_by_xpath(xpath).click()
    # time.sleep(5)
    #
    # xpath = '//*[@id="react-root"]/section/main/div/div/div/div/button'
    # driver.find_element_by_xpath(xpath).click()
    # time.sleep(5)

    # 총 게시물 숫자 불러오기
    page_string = driver.page_source
    bs_obj = BeautifulSoup(page_string, 'lxml')
    temp_data = str(bs_obj.find_all(name='meta')[-1])
    start = temp_data.find('게시물') + 4
    end = temp_data.find('개')
    total_data = temp_data[start:end]

    print("총 {0}개의 게시물이 검색되었습니다.".format(total_data))
    print("게시물을 수집하는 중입니다.")

    # 사양에 맞게 스크롤 후 일시정지 시간 1.0 ~ 2.0 사이로 수정
    scroll_pause_time = 2.0
    reallink = []
    cnt = 0

    # 멀티프로세싱
    # 사양에 맞게 프로세스 수 결정
    process_num = 10
    lock = Lock()

    # 한번에 처리할 게시글 수
    block = 1000

    while True:
        page_string = driver.page_source
        bs_obj = BeautifulSoup(page_string, 'lxml')

        for link1 in bs_obj.find_all(name='div', attrs={"class": "Nnq7C weEfm"}):
            for i in range(3):
                try:
                    title = link1.select('a')[i]
                    real = title.attrs['href']
                    reallink.append(real)
                except IndexError:
                    pass

        length = len(reallink)
        print(f'{cnt + length}번째 게시글까지 검색함')

        if length > block:
            cnt += length
            print(f'{cnt}번째 게시글 까지 분석하는중')
            process_list = []
            for process_idx in range(process_num):
                ps = Process(target=parsing, args=(lock, process_num, process_idx, reallink, tags))
                process_list.append(ps)
                ps.start()
            for i in range(len(process_list)):
                process_list[i].join()
            reallink = []

        last_height = driver.execute_script('return document.body.scrollHeight')
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(scroll_pause_time)
        new_height = driver.execute_script("return document.body.scrollHeight")
        time.sleep(scroll_pause_time)

        if new_height == last_height:
            print('마지막 게시글 까지 분석하는중')
            if length > block:
                process_list = []
                for process_idx in range(process_num):
                    ps = Process(target=parsing, args=(lock, process_num, process_idx, reallink, tags))
                    process_list.append(ps)
                    ps.start()
                for i in range(len(process_list)):
                    process_list[i].join()
            break
        else:
            pass

    driver.close()
    driver.quit()


if __name__ == '__main__':
    main()