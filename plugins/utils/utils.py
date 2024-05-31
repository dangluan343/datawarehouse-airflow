import math
# Định nghĩa hàm assign_key
def assign_keys(data, key_names, key_values):
    """
    Hàm này gán các trường mới vào mỗi đối tượng trong mảng.

    Parameters:
    - data(list): Mảng các đối tượng (dict).
    - key_names(list): Mảng tên các trường mới cần gán.
    - key_values(list): Mảng giá trị của trường mới.

    Returns:
    Không có giá trị trả về. Hàm thay đổi trực tiếp các đối tượng trong mảng data.
    """
    for obj in data:
        for key, value in zip(key_names, key_values):
            obj[key] = value

