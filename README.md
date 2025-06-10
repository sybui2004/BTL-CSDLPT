# BTL CSDLPT: MÔ TẢ PHÂN MẢNH DỮ LIỆU

- [BTL CSDLPT: MÔ TẢ PHÂN MẢNH DỮ LIỆU](#btl-csdlpt-mô-tả-phân-mảnh-dữ-liệu)
  - [1. Giới thiệu](#1-giới-thiệu)
  - [2. Nội dung](#2-nội-dung)
  - [3. Cấu trúc dự án](#3-cấu-trúc-dự-án)
  - [4. Hướng dẫn cài đặt](#4-hướng-dẫn-cài-đặt)
  - [5. Tham khảo](#5-tham-khảo)

## 1. Giới thiệu
Mô tả phân mảnh dữ liệu là bài tập lớn môn cơ sở dữ liệu phân tán, yêu cầu phân mảnh 10 triệu dòng dữ liệu theo hai kiểu phân mảnh ngang.

**Thành viên nhóm:**
|STT|Họ tên|Mã Sinh viên|
|---|---|---|
|1|Bùi Thái Sỹ|B22DCCN702|
|2|Nguyễn Thành Công|B22DCCN090|
|3|Hoàng Hải Long|B22DCCN496||

## 2. Nội dung

|Phần|Nội dung|Sinh viên thực hiện|
|---|---|---|
|1|Nhóm trưởng, phân chia công việc, tối ưu hàm loadratings(), cải thiện hiệu suất các hàm.|Bùi Thái Sỹ|
|2|Tối ưu hàm rangeinsert() và roundrobininsert() sử dụng metadata trong hai hàm partition().|Nguyễn Thành Công|
|3|Tối ưu rangepartition() ,roundrobinpartition() dùng unlogged table và tối ưu truy vấn.|Hoàng Hải Long|

## 3. Cấu trúc dự án
```plain text
├───README.md                           # Mô tả dự án
├───Assignment1Tester.py                # File test
├───testHelper.py                       # File test
├───Interface_Sample.py                 # Solution gốc
├───Interface.py                        # Solution tối ưu
├───loadratingsupdate.py                # Các phiên bản hàm loadratings()
├───rangepartitionupdate.py             # Các phiên bản hàm rangepartition()
├───roundrobinpartitionupdate.py        # Các phiên bản hàm roundrobinpartition()
├───test_data.dat                       # Dữ liệu test
├───requirements.txt                    # Các thư viện cần cài đặt
├───Đề bài.docx                         # Đề bài
└───Báo cáo CSDLPT.docx                 # Báo cáo
```

## 4. Hướng dẫn cài đặt

**Cài đặt các thư viện hỗ trợ**
1. Mở thư mục `BTL-CSDLPT` trên cmd
2. Cài đặt các thư viện trong requirements.txt
    ```bash
    pip install -r .\requirements.txt 
    ```

**Cài đặt PostgreSQL**
[Link](https://www.postgresql.org/download/windows/)

**Mô phỏng phân mảnh**
1. Mở thư mục `BTL-CSDLPT` trên cmd
2. Chạy file Assignment1Tester.py
    ```bash
    python Assignment1Tester.py
    ```

## 5. Tham khảo
* [Sử dụng Polars và DuckDB để tối ưu hàm loadratings()](https://www.youtube.com/watchv=utTaPW32gKY)
* [Dùng bảng UNLOGGED TABLE](https://www.postgresql.org/docs/current/sql-createtable.html)
* [StringIO](https://www.geeksforgeeks.org/stringio-module-in-python/) 
* [Thư viện psycopg2](https://www.psycopg.org/docs/)
* [Giảm độ trễ cho Postgre](https://www.postgresql.org/docs/current/wal-async-commit.html)
* [Cấu hình Postgre](https://dangxuanduy.com/database/cac-tham-so-cau-hinh-memory-trong-postgresql/)
* [Polars Dataframe](https://docs.pola.rs/api/python/stable/reference/dataframe/index.html)
* [DuckDB](https://duckdb.org/docs/stable/clients/python/dbapi.html) 