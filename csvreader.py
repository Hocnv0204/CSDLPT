import csv 
#  filePath là đường dẫn tuyệt đối đến tệp ratings.dat
 with open(filePath, 'r', encoding='utf-8') as f:
     reader = csv.reader(f, delimiter='::', quoting=csv.QUOTE_NONE)
     for row in reader:
        # row sẽ là một list các chuỗi, ví dụ ['1', '122', '5', '838985046']
        # Cần chuyển đổi kiểu dữ liệu ở đây
        try:
             userID = int(row)
             movieID = int(row)
             rating_val = float(row)
             timestamp_val = int(row)
             # Xử lý dữ liệu đã chuyển đổi
        except ValueError as e:
         # Ghi nhận hoặc bỏ qua dòng lỗi
            print(f"Bỏ qua dòng lỗi định dạng: {row} do {e}")
        except IndexError as e:
            # Ghi nhận hoặc bỏ qua dòng thiếu trường
            print(f"Bỏ qua dòng thiếu trường: {row} do {e}")