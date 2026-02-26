import gspread
from oauth2client.service_account import ServiceAccountCredentials

SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1hJoa1sjXbFkJlKeSEwQDW2YKOLDXBaaxfP_FyiyhYEQ/edit?gid=0#gid=0"
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]


def save_to_google_sheets(results, store_label):
    if not results:
        print("수집된 데이터가 없어 구글 시트에 기록하지 않습니다.")
        return

    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", SCOPE)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(SPREADSHEET_URL).get_worksheet(0)

        existing_rows = sheet.get_all_values()
        data_rows = existing_rows[1:] if existing_rows and existing_rows[0][0] in ("스토어", "Store") else existing_rows
        existing_keys = {
            (row[0].strip(), row[4].strip())
            for row in data_rows if len(row) >= 5
        }
        print(f"기존 시트 데이터: {len(existing_keys)}개 (스토어+상품코드 기준)")

        new_values = []
        skipped = 0
        for item in results:
            store = str(item.get("스토어", ""))
            code = str(item.get("상품코드", ""))
            if (store, code) in existing_keys:
                skipped += 1
                continue
            new_values.append(list(map(str, item.values())))
            existing_keys.add((store, code))

        print(f"중복 제외: {skipped}개 / 신규 추가 대상: {len(new_values)}개")

        if new_values:
            sheet.append_rows(new_values)
            print(f"구글 스프레드시트에 {len(new_values)}개의 {store_label} 신규 상품을 기록했습니다.")
        else:
            print("신규 상품이 없어 시트에 기록하지 않습니다.")
    except Exception as e:
        print(f"구글 스프레드시트 기록 중 에러 발생: {e}")
