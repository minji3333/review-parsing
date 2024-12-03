import json
import mysql.connector
import os
import glob
import random
from datetime import datetime, timedelta

MAIN_CATEGORY = {
    "영상/음향가전": 26,
    "생활/미용/욕실가전": 27,
    "주방가전": 28,
    "계절가전": 29,
}

# MySQL 데이터베이스 연결 설정
db = mysql.connector.connect(
    host="localhost",
    user="root",  # DB 사용자 이름
    password="1234",  # DB 비밀번호
    database="chatbot"
)

cursor = db.cursor()

# JSON 데이터 삽입 함수 정의
def insert_review_data(json_file_path):
    with open(json_file_path, 'r', encoding='utf-8') as f:
        reviews_data = json.load(f)

    for review in reviews_data:
        product_name = review["ProductName"]
        main_category = review["MainCategory"]
        domain = review["Domain"]

        # 제품 테이블에 제품이 이미 있는지 확인하고 없으면 추가
        cursor.execute(
            "SELECT id FROM products WHERE name = %s",
            (product_name,)
        )
        result = cursor.fetchone()

        if result:
            product_id = result[0]
        else:
            photo = "https://img.hankyung.com/photo/202406/01.36942977.1.jpg"
            manufacturer = random.choice(["Samsung", "LG", "Sony", "Panasonic", "Philips", "Toshiba"])
            release_year = (datetime.now() - timedelta(days=random.randint(0, 3650))).year
            energy_efficiency = f"{random.randint(1, 5)}등급"
            power_consumption = round(random.uniform(50, 500), 2)
            weight = round(random.uniform(0.5, 10), 2)
            price = random.randint(100000, 5000000)

            cursor.execute(
                "INSERT INTO products (name, sub_category_id, photo, manufacturer, release_year, energy_efficiency, power_consumption, weight, price) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (product_name, MAIN_CATEGORY[main_category], photo, manufacturer, release_year, energy_efficiency, power_consumption, weight, price)
            )
            product_id = cursor.lastrowid

        # GeneralPolarity가 없는 경우 기본값 0으로 설정
        general_polarity = review.get("GeneralPolarity", 0)

        # 리뷰 테이블에 리뷰 추가
        cursor.execute(
            """
            INSERT INTO reviews (
                product_id, raw_text, source, review_score, syllable_count, word_count, review_date, general_polarity
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                product_id, review["RawText"], review["Source"], review["ReviewScore"],
                review["Syllable"], review["Word"], review["RDate"], general_polarity
            )
        )
        review_id = cursor.lastrowid

        # 각 리뷰에 대한 속성 추가 및 관계 설정
        for aspect_data in review["Aspects"]:
            aspect = aspect_data["Aspect"]
            sentiment_text = aspect_data["SentimentText"]
            sentiment_word_count = aspect_data["SentimentWord"]
            sentiment_polarity = aspect_data["SentimentPolarity"]

            # Aspects 테이블에 속성이 이미 있는지 확인하고 없으면 추가
            cursor.execute(
                "SELECT id FROM aspects WHERE aspect = %s",
                (aspect,)
            )
            aspect_result = cursor.fetchone()

            if aspect_result:
                aspect_id = aspect_result[0]
            else:
                cursor.execute(
                    "INSERT INTO aspects (aspect) VALUES (%s)",
                    (aspect,)
                )
                aspect_id = cursor.lastrowid

            # ReviewAspects 테이블에 리뷰와 속성의 관계 추가
            cursor.execute(
                """
                INSERT INTO review_aspects (
                    review_id, aspect_id, sentiment_text, sentiment_word_count, sentiment_polarity
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (review_id, aspect_id, sentiment_text, sentiment_word_count, sentiment_polarity)
            )

# "03. 가전" 폴더 내의 모든 JSON 파일을 검색하여 데이터 삽입
base_directory = "03. 가전"  # 폴더 경로를 지정하세요.

# 각 하위 폴더를 탐색하여 JSON 파일들을 찾음
for subfolder in os.listdir(base_directory):
    subfolder_path = os.path.join(base_directory, subfolder)
    if os.path.isdir(subfolder_path):
        # 하위 폴더 내의 모든 JSON 파일을 검색
        json_files = glob.glob(os.path.join(subfolder_path, "*.json"))
        for json_file in json_files:
            print(f"Inserting data from: {json_file}")
            insert_review_data(json_file)

# 변경 사항 커밋 및 연결 종료
db.commit()
cursor.close()
db.close()

print("All data has been successfully inserted into the database.")
