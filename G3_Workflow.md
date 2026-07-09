# **🔄 QUY TRÌNH LÀM VIỆC CỦA DATA TEAM (5 THÀNH VIÊN)**

## **📝 2\. CHI TIẾT CÁC BƯỚC THỰC HIỆN**

### **💻 Giai đoạn 1: Lập trình tại Local (VS Code & GitHub)**

*Mục tiêu: Viết code, tận dụng AI và lưu trữ an toàn rẽ nhánh riêng biệt.*

1. **Đồng bộ Code (Pull Main):** \* Mở VS Code, chuyển sang nhánh main: git checkout main  
   * Cập nhật code mới nhất: git pull origin main  
2. **Tạo nhánh cá nhân (Branching):**  
   * Tạo nhánh làm việc riêng để không đụng code của 4 thành viên còn lại.  
   * Lệnh: git checkout \-b \<tên-nhánh-của-bạn\> (VD: melvin)  
3. **Sinh Code với AI (AI Coding):**  
   * Sử dụng AI trong VS Code  để hỗ trợ.  
   * Tinh chỉnh code cho phù hợp với logic nghiệp vụ.  
4. **Lưu & Đẩy code (Push):**  
   * Commit thay đổi: git commit \-m "Thêm logic xử lý bảng Sales"  
   * Push nhánh cá nhân lên GitHub: git push origin \<tên-nhánh-của-bạn\> (ex: melvin)

### **🧱 Giai đoạn 2: Kiểm thử (Databricks DEV/TEST)**

*Mục tiêu: Chạy code từ nhánh cá nhân trên môi trường phân tán để bắt lỗi sớm.*

5. **Kéo code về Workspace:**  
   * Mở Databricks (phần Repos/Workspace), mở repository của dự án.  
   * Chuyển sang **nhánh cá nhân** của bạn (ex: melvin ) và bấm **Pull**.  
6. **Chạy thử nghiệm (Catalog DEV):**  
   * Mở Notebook, chọn Widget (Dropdown) giá trị: Catalog \= g3_dev.  
   * Chạy toàn bộ Notebook (Run All) để kiểm tra cú pháp và lỗi logic cơ bản.  
7. **Kiểm tra nghiệp vụ (Catalog TEST):**  
   * Nếu bước DEV ổn, đổi Widget (Dropdown) thành: Catalog \= g3_test.  
   * Chạy lại Notebook.  
   * *Mục đích:* Dữ liệu ở TEST gần giống thật hơn, giúp kiểm chứng kết quả nghiệp vụ cùng với team/QA.

### **🚀 Giai đoạn 3: Tích hợp và Triển khai (Merge & PROD)**

*Mục tiêu: Bảo vệ môi trường Product, đảm bảo chỉ chạy code đã được review và chuẩn hóa.*

8. **Tạo Pull Request & Merge Code (GitHub):**  
   * Lên GitHub, tạo **Pull Request (PR)** từ nhánh cá nhân (ex: melvin).  
   * Các thành viên trong team review code.  
   * Khi được duyệt, tiến hành **Merge** code vào nhánh main.  
9. **Cập nhật code chính trên Databricks:**  
   * Quay lại Databricks Repos.  
   * Chuyển nhánh làm việc về lại nhánh **main**.  
   * Bấm **Pull** để lấy bản code hoàn chỉnh (vừa được merge) về Workspace.  
10. **Chạy thật (Catalog PROD):**  
    * Mở Notebook (lúc này đang ở nhánh main), đổi Widget (Dropdown) thành: Catalog \= g3_catalog.  
    * Chạy Notebook để nạp dữ liệu chính thức vào hệ thống.