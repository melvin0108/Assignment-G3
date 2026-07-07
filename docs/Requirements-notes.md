Dựa trên nội dung file ghi âm bạn cung cấp, đây là một buổi hướng dẫn (briefing) và giao bài tập (assignment) thực hành chuyên sâu về vị trí Core Data Engineer do một người hướng dẫn (mentor/quản lý) trình bày cho các thành viên/học viên trong dự án.
Dưới đây là phần tổng hợp và phân tích chi tiết toàn bộ nội dung mà không bỏ sót bất kỳ phần nào:
1. Tổng quan 4 nhiệm vụ cốt lõi của Assignment
Người hướng dẫn nhấn mạnh mục tiêu chính của bài tập này là xây dựng core pipeline cho Data Engineer, bao gồm 4 công việc chính
:
Nhiệm vụ 1: Chuẩn bị Mock Dataset (Dữ liệu giả lập): Các nhóm sẽ tự tạo hoặc thu thập dữ liệu thuộc 1 trong 4 chủ đề (ví dụ: customer, fraud detection...)
. Dữ liệu phải có quy mô khoảng 20 đến 30 bảng
 và bắt buộc phải chứa các lỗi về Data Quality để phục vụ việc thực hành xử lý
. Có thể dùng dữ liệu từ Kaggle, gọi API hoặc tự viết script tạo dữ liệu (dummy data)
.
Nhiệm vụ 2: Thiết kế Data Pipeline: Xây dựng luồng xử lý từ việc kết nối lấy dữ liệu (ingestion), đưa vào Raw layer và đưa lên tầng Silver
. Các job etl cần chạy theo cơ chế Batch (vì là mock data)
.
Nhiệm vụ 3: Thiết lập Data Quality / Data Contract: Tự định nghĩa các quy tắc nghiệp vụ (business rules) để kiểm tra chất lượng dữ liệu. Khi phát hiện dữ liệu lỗi (bad records), pipeline phải có cơ chế bắt lỗi và xử lý rõ ràng
.
Nhiệm vụ 4: Transformation lên tầng Silver Modeling: Transform dữ liệu lên tầng Silver, người hướng dẫn gợi ý tham khảo tài liệu IBM Data Modeling để thiết kế các bảng sao cho chuẩn chỉnh
.
2. Yêu cầu về Công nghệ & Kỹ thuật (Technical Stack)
Môi trường xử lý: Khuyến khích sử dụng Databricks Free Edition để sát với thực tế công việc
. Ngoài ra, các nhóm hoàn toàn có thể chạy dưới local sử dụng Pandas, Polars, hoặc các database nội bộ như DuckDB, PostgreSQL
. Không được sử dụng tài khoản AWS cá nhân để làm, vì trong môi trường các công ty thực tế sẽ không cho phép điều này, mọi thứ nên được chạy trực tiếp trên máy cá nhân
.
Công cụ Data Quality: Có thể tự code một framework đơn giản, hoặc sử dụng các framework có sẵn trên thị trường như Great Expectations, Soda, Deequ, hoặc DQS của Databricks
.
3. Các yêu cầu nâng cao & Điểm cộng (Optional/Advanced)
Dù là phần không bắt buộc hoặc chiếm tỷ trọng điểm thấp, nhưng người hướng dẫn rất khuyến khích thực hiện để hoàn thiện bức tranh hệ thống:
Data Privacy (Bảo mật dữ liệu): Có hướng xử lý dữ liệu nhạy cảm (sensitive data) bằng các kỹ thuật như Data Masking hoặc Labeling. Tùy thuộc vào role (vai trò) của user mà họ sẽ được xem dữ liệu thật hoặc dữ liệu đã bị che (masked)
.
Data Lineage & Metadata: Cần truy xuất được nguồn gốc end-to-end của dữ liệu. Cụ thể: dữ liệu lấy từ bảng nào, qua những bước transform nào, áp dụng business rule nào
.
AI Ready Context: Yêu cầu nhóm viết các tài liệu cấu hình (context) để các công cụ AI (như Cursor, GitHub Copilot) có thể đọc và hiểu được cấu trúc dữ liệu của project. Chỉ cần viết dưới dạng file Markdown (.md) thể hiện rõ thông tin, không yêu cầu thiết lập phức tạp như Vector Database hay Ontology
.
Unit Test: Phải viết test để kiểm thử xem hệ thống sẽ phản ứng ra sao khi có schema mới phát sinh hoặc khi dữ liệu vi phạm business rules
.
4. Yêu cầu sản phẩm bàn giao (Deliverables)
Kết quả cuối cùng nộp lại sẽ là một Git Repository bao gồm:
Cấu hình (Config) sinh mock dataset
. (Người hướng dẫn rất muốn xin lại bộ source code sinh data/metadata của các nhóm để tái sử dụng cho các bài toán/project khác trong tương lai)
.
Cấu hình của Data Pipeline và Data Quality
.
Các rule Transformation dữ liệu
.
Tài liệu Markdown (Readme/Context)
.
5. Tổ chức nhóm, Khối lượng công việc & Thời gian
Quy mô nhóm: Dự kiến từ 5 đến 8 người/nhóm, có thể phân chia công việc: người làm mock data, người thiết kế framework pipeline, người làm data quality, người setup môi trường
.
Volume & Stress test: Dữ liệu tạo ra cần có độ lớn tương đối để chạy stress test (ví dụ: dữ liệu sinh ra trong 1 tiếng tương đương 2 triệu records tùy bài toán)
.
Thời gian (Timeline): Đã có một sự cố nhỏ trong buổi họp khi học viên báo rằng họ chỉ có 2 tuần để làm Assignment này
. Người hướng dẫn bất ngờ vì 2 tuần là quá ngắn để làm hết scope việc này (đặc biệt là khâu sinh dữ liệu tốn rất nhiều thời gian). Anh hứa sẽ thảo luận lại với quản lý (anh Kỳ) để cắt giảm bớt scope hoặc gia hạn thêm thời gian
.
Lưu ý về Final Project: Có học viên hỏi đây có phải dự án cuối khóa không. Câu trả lời là Không. Dự án cuối khóa sẽ đòi hỏi tính quy mô End-to-End bao trùm hơn: bao gồm cả Platform Engineer (dùng Terraform deploy), làm Dashboard, v.v. Còn bài này chỉ thuần túy focus vào Core Data Engineer
.
6. Thảo luận nội bộ của học viên ở cuối buổi họp
Sau khi người hướng dẫn kết thúc phần trình bày
, phần cuối file ghi âm là cuộc trò chuyện tự do của các thành viên trong nhóm:
Các bạn chia sẻ cách tối ưu thời gian bằng việc góp tiền mua tài khoản AI cao cấp (ChatGPT Plus / Cursor / "con trả tiền") để AI hỗ trợ viết code sinh mock data và setup hệ thống cho nhanh
.
Một số thành viên rủ nhau học thêm các khóa học bổ trợ về Databricks, Platform, viết CI/CD, Github Actions thay vì chỉ phụ thuộc vào AI chatbot đơn thuần để thực sự nâng cao hiệu suất làm việc
.