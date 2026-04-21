# Hướng dẫn CI/CD và Viết Test cho nhóm

> Đọc một lần, làm theo là xong. Không cần kinh nghiệm trước.

---

## 1. Quy trình làm việc hàng ngày

**Mỗi khi bắt đầu code:**
```bash
git checkout main
git pull origin main
git checkout -b feature/ten-ban-hoac-tinh-nang
```

**Trong lúc code:**
```bash
git add .
git commit -m "feat: mô tả ngắn gọn bạn làm gì"
git push origin feature/ten-ban-hoac-tinh-nang
```

**Khi xong việc:**
1. Vào GitHub → tạo **Pull Request** vào nhánh `main`
2. Chờ CI chạy (góc phải dưới PR có vòng tròn xanh/đỏ)
3. Nếu **xanh** → nhắn nhóm review → merge
4. Nếu **đỏ** → đọc mục 3 bên dưới để fix

> **Không được push thẳng vào `main`.**

---

## 2. CI là gì và nó kiểm tra gì?

CI (Continuous Integration) là robot tự động chạy mỗi khi bạn tạo Pull Request.

| Job CI | Kiểm tra gì | Thường fail vì |
|--------|-------------|----------------|
| `Lint Python` | Code Python có đúng chuẩn không | Thừa dấu cách, import thừa, v.v. |
| `Validate Docker Compose` | File `docker-compose.yml` có đúng không | Sai cú pháp YAML |
| `Validate Kubernetes` | File trong `k8s/` có đúng không | Sai cú pháp YAML |
| `Run Tests` | Các test trong `tests/` có pass không | Code bị broken |

---

## 3. CI đỏ thì làm gì?

### Lỗi Lint (ruff)
```bash
pip install ruff
ruff check .        # xem lỗi ở đâu
ruff check . --fix  # tự sửa tự động
git add . && git commit -m "fix: lint" && git push
```

### Lỗi Docker Compose
```bash
docker compose config  # đọc báo lỗi dòng nào trong docker-compose.yml
```

### Lỗi Test
```bash
pip install pytest
pytest tests/ -v  # chạy local, đọc dòng FAILED để biết test nào lỗi
```

---

## 4. Chạy test trên máy local

```bash
# Cài pytest (chỉ cần làm 1 lần)
pip install pytest

# Chạy tất cả test
pytest tests/

# Chạy 1 file test cụ thể
pytest tests/test_mock_generator.py -v

# Xem kết quả chi tiết
pytest tests/ -v
```

---

## 5. Cài pre-commit (chỉ làm 1 lần trên máy mỗi người)

Pre-commit tự sửa lint lỗi mỗi khi bạn `git commit` — không bao giờ bị CI đỏ vì lint nữa.

```bash
pip install pre-commit
pre-commit install
```

Xong. Từ giờ mỗi lần `git commit` nó tự kiểm tra.

---

## 6. Muốn viết thêm test — làm thế nào?

Mở file `tests/test_mock_generator.py` xem cấu trúc mẫu. Mỗi test là 1 hàm bắt đầu bằng `test_`.

**Template cơ bản:**
```python
def test_ten_ro_rang_mo_ta_gia_tri_gi():
    # Chuẩn bị dữ liệu
    dau_vao = ...

    # Chạy hàm cần test
    ket_qua = ham_can_test(dau_vao)

    # Kiểm tra kết quả đúng không
    assert ket_qua == gia_tri_mong_doi
```

**Ví dụ thực tế:**
```python
def test_tinh_cpc():
    budget = 1_000_000   # 1 triệu VND
    clicks = 500
    cpc = budget / clicks
    assert cpc == 2000.0
```

**Quy tắc đặt tên:**
- File: `test_ten_module.py` (ví dụ: `test_mock_generator.py`)
- Hàm: `test_mo_ta_dieu_can_kiem_tra` (dùng tiếng Việt hay tiếng Anh đều được)

---

## 7. Ai cần viết test?

| Bạn code cái gì | Bạn viết test cho cái đó |
|-----------------|--------------------------|
| Hàm xử lý data | ✅ Nên viết |
| Config Airflow DAG | Đã có `test_dag.py` tự động kiểm tra |
| Docker Compose / K8s YAML | CI validate tự động |
| SQL ClickHouse | Không cần test |

> Không ai bắt buộc phải viết test nhiều. 2-3 test cho phần mình code là đủ để báo cáo.
