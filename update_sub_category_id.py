import re
import mysql.connector

SUB_CATEGORIES = {
    1: ["TV", "스탠드", "Crystal UHD", "QLED", "Neo QLED"],
    2: ["빔프로젝터", "프로젝터", "시네빔", "BenQ", "Epson"],
    3: ["블루투스 이어폰", "TWS", "에어팟", "헤드폰", "이어셋", "이어폰"],
    4: ["무선 스피커", "스피커", "Soundbar", "휴대용", "BOSE"],
    5: ["사운드바", "멀티빔"],
    6: ["오디오/플레이어", "오디오", "플레이어", "MP3", "턴테이블", "마이크", "노래방", "앰프"],
    7: ["세탁기", "트롬", "그랑데", "워시타워"],
    8: ["의류건조기", "건조기", "그랑데", "DV", "RH", "드럼"],
    9: ["의류관리기", "스타일러", "의류케어"],
    10: ["신발관리기", "신발"],
    11: ["무선청소기", "코드제로", "다이슨", "스틱청소기", "제트", "청소기"],
    12: ["로봇청소기", "로보락", "샤오미", "에코백스", "디봇"],
    13: ["공기청정기", "위닉스", "에어", "HEPA", "공기정화기"],
    14: ["냉장고", "양문형", "비스포크", "냉동고", "오브제컬렉션"],
    15: ["김치냉장고", "김치", "뚜껑형", "딤채"],
    16: ["식기세척기", "식기", "세척기"],
    17: ["정수기", "필터", "냉온수기"],
    18: ["전기레인지", "인덕션", "하이라이트", "더 플레이트"],
    19: ["오븐/전자레인지", "오븐", "전자레인지", "레인지", "스팀오븐"],
    30: ["밥솥", "압력"],
    31: ["에어프라이어"],
    20: ["에어컨"],
    21: ["히터"],
    22: ["가습기"],
    23: ["제습기"],
    24: ["선풍기", "써큘레이터", "팬", "냉풍기", "서큘레이터", "서큘에이터", "써큘"],
    25: ["온풍기", "난방기", "열풍기"],
}

# MySQL 데이터베이스 연결 설정
db = mysql.connector.connect(
    host="localhost",
    user="root",  # DB 사용자 이름
    password="1234",  # DB 비밀번호
    database="chatbot"
)

cursor = db.cursor()

def get_sub_categories():
    cursor.execute("SELECT id, name FROM sub_categories")
    sub_categories = cursor.fetchall()

    for sub_category_id, sub_category_name in sub_categories:
        print(f"{sub_category_id}: \"{sub_category_name}\",")


def get_sub_category_id(product_name, sub_categories):
    for sub_key, keywords in sub_categories.items():
        for keyword in keywords:
            if keyword in product_name:
                return sub_key
    return None


def get_sub_category_id(product_name):
    for sub_key, keywords in SUB_CATEGORIES.items():
        for keyword in keywords:
            if keyword in product_name:
                return sub_key
    return None


# get products
cursor.execute("SELECT id, name, sub_category_id FROM products")
products = cursor.fetchall()

successed = 0
failed = 0
for product_id, product_name, sub_category_id in products:
    cleaned_name = re.sub(r"\([^\)]*\)\s*", "", product_name)
    sub_category_id = get_sub_category_id(cleaned_name)

    cursor.execute("UPDATE products SET name = %s WHERE id = %s", (cleaned_name, product_id))
    
    if sub_category_id:
        cursor.execute("UPDATE products SET sub_category_id = %s WHERE id = %s", (sub_category_id, product_id))
        # print(product_id, sub_category_id, cleaned_name)
        successed += 1
    else:
        # print(product_id, cleaned_name)
        failed += 1

print(successed, failed)

# 변경 사항 커밋 및 연결 종료
db.commit()
cursor.close()
db.close()

print("All data has been successfully updated.")
