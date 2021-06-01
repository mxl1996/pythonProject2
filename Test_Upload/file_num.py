import glob


def get_file_name(path=None):  # 可变参数
    """
    获取文件夹内文件的名字
    :param path: 文件夹绝对路径+文件模糊名
    :return: 文件:文件夹绝对路径+文件模糊名list
    """
    path_file_name = glob.glob(pathname=path)
    return path_file_name


def get_file_num(path=None):
    """
    获取文件夹内文件的数量
    :param path: 文件夹绝对路径+文件模糊名
    :return: 文件个数
    """
    path_file_number = glob.glob(pathname=path)
    return len(path_file_number)

if __name__ == '__main__':
    print(get_file_name(path='/Users/blank/Documents/导入/*.json'))


