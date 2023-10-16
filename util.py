import getpass
import sys,os,logging,json
import hashlib
import winreg
import platform
import subprocess
import ctypes
import ctypes as ct
from ctypes import wintypes as w
import multiprocessing

app_path = os.path.dirname(os.path.abspath(sys.argv[0]))

def read_registry_value(key_path, value_name):
    try:
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path)
        value, _ = winreg.QueryValueEx(key, value_name)
        winreg.CloseKey(key)
        return value
    except FileNotFoundError:
        print("Registry key not found.")
    except PermissionError:
        print("Permission denied.")
    except Exception as e:
        print("Error occurred:", str(e))


def get_wechat_path():
    try:
        user = getpass.getuser()
        dirs = [] #函数返回结果，存放微信存储数据的目录（可能又历史遗留，多个地方装了，这个就都扫描一遍，返回一个路径的数组）
        dic = {
            'pc': 'C:\\Users\\' + user + '\\Documents\\WeChat Files',
            'forwin10': 'C:\\Users\\' + user + '\\AppData\\Local\\Packages\\TencentWeChatLimited.forWindows10_sdtnhv12zgd7a\\LocalCache\\Roaming\\Tencent\\WeChatAppStore\\WeChatAppStore Files',
            'foruwp': 'C:\\Users\\' + user + '\\AppData\\Local\\Packages\\TencentWeChatLimited.WeChatUWP_sdtnhv12zgd7a\\LocalCache\\Roaming\\Tencent\\WeChatAppStore\\WeChatAppStore Files'
        }
        for key in dic:
            if os.path.exists(dic[key]):
                dirs.append(dic[key])
        # 注册表路径和字段名
        registry_key_path = r"software\tencent\wechat"
        value_name = "FileSavePath"
        # 读取注册表里面微信的存储路劲
        value = read_registry_value(registry_key_path, value_name)
        if not value in dirs:
            dirs.append(value)
        return dirs
    except Exception as e:
        return [os.getcwd()] #如果没获得到微信数据目录，那么返回当前程序所在目录，否则返回None，不太友好

def get_fs_type_old(path): #给定一个path，获得这个path所在文件系统的类型
    system = platform.system()
    if system == 'Windows':
        # 设置命令行编码为437（英文）
        subprocess.run('chcp 437', shell=True) #修改page页，后续命令英文输出
        drive, _ = os.path.splitdrive(path)
        command = f'fsutil fsinfo volumeinfo {drive}'
        output = subprocess.check_output(command, shell=True).decode()
        # 提取文件系统类型
        file_system_type = ""
        for line in output.split('\n'):
            if "File System Name : " in line:
                file_system_type = line.split("File System Name : ")[1].strip().lstrip().lower()
                break
        return file_system_type
    elif system == 'Linux':
        command = 'df -PT '+path
        output = subprocess.check_output(command, shell=True).decode()
        # 解析Linux df命令的输出，获取文件系统类型
        lines = output.split('\n')
        if len(lines) > 1:
            fields = lines[1].split()
            if len(fields) >= 2:
                file_system_type = fields[1]
                return file_system_type.lower()
    return "Unknown"

def get_fs_type(path): #给定一个path，获得这个path所在文件系统的类型
    system = platform.system()
    try:
        if system == 'Windows':
            if system == 'Windows':
                drive, _ = os.path.splitdrive(path)
                target_disk=drive+"\\"
                volumeNameBuffer = ct.create_unicode_buffer(w.MAX_PATH + 1)
                fileSystemNameBuffer = ct.create_unicode_buffer(w.MAX_PATH + 1)
                volume_name_buffer = ctypes.create_unicode_buffer(drive)
                serial_number = w.DWORD()
                max_component_length = w.DWORD()
                file_system_flags = w.DWORD()
                result = ctypes.windll.kernel32.GetVolumeInformationW(
                    target_disk,
                    volumeNameBuffer, ct.sizeof(volumeNameBuffer),
                    ct.byref(serial_number),
                    ct.byref(max_component_length),
                    ct.byref(file_system_flags),
                    fileSystemNameBuffer, ct.sizeof(fileSystemNameBuffer))
                print(f"{result=},{serial_number.value=},{file_system_flags.value=},{fileSystemNameBuffer.value=}")
                if result != 0:
                    return fileSystemNameBuffer.value.lower()
        elif system == 'Linux':
            command = 'df -PT '+path
            output = subprocess.check_output(command, shell=True).decode()
            # 解析Linux df命令的输出，获取文件系统类型
            lines = output.split('\n')
            if len(lines) > 1:
                fields = lines[1].split()
                if len(fields) >= 2:
                    file_system_type = fields[1]
                    return file_system_type.lower()
        return "Unknown"
    except Exception as e:
        print(f"ERROR:{e}")
        return None

def get_logger(log_file):
    # 定log输出格式，配置同时输出到标准输出与log文件，返回logger这个对象
    logger = logging.getLogger('mylogger')
    logger.setLevel(logging.DEBUG)
    log_format = logging.Formatter(
        '%(asctime)s - %(filename)s- %(levelname)s - %(message)s')
    log_fh = logging.FileHandler(log_file)
    log_fh.setLevel(logging.DEBUG)
    log_fh.setFormatter(log_format)
    log_ch = logging.StreamHandler()
    log_ch.setLevel(logging.DEBUG)
    log_ch.setFormatter(log_format)
    logger.addHandler(log_fh)
    logger.addHandler(log_ch)
    return logger


def get_cpu_cores(): #获取cpu的核数，用于后面给允许几个线程算md5做参考
    try:
        # 使用os模块获取CPU核数
        num_cores = os.cpu_count()
        if num_cores is None:
            # 如果os.cpu_count()返回None，使用multiprocessing模块获取CPU核数
            num_cores = multiprocessing.cpu_count()
        return num_cores
    except Exception as e:
        return 1  #如果异常的话，也要给个数字1

def get_cfg(cfg_file): #读取配置文件，获得系统配置
    cfg={}
    if os.path.isfile(cfg_file):
        ss=open(cfg_file,'r',encoding='utf8').read()
        cfg=json.loads(ss)
    else:
        cfg['dirs']=get_wechat_path() #尝试获得微信的存储目录，作为要被扫描的目录
        cfg['cache_file'] = os.path.join(app_path,'cache.dat')  #cache文件存放位置，默认放在当前程序目录下，如果空间紧张，可以把它放别处
        cfg['md5_key_file']=os.path.join(app_path,'md5_key_files.dat')  #以md5为key的hash dict，文件存放路径
        cfg['to_del_file'] = os.path.join(app_path, 'to_del_files.dat')  # 以md5为key的hash dict，但是存放的只是要清理的文件信息，存放路径
        cfg['ask_before_del']=True #批量删除前，先进行确认下
        cfg['max_workers']=get_cpu_cores() #起多个线程计算md5
        open(cfg_file,'w',encoding='utf8').write(json.dumps(cfg,indent=2,ensure_ascii=True))
    return cfg

def md5_file(file_path): #计算文件的md5值
    with open(file_path, 'rb') as f:
        md5_hash = hashlib.md5()
        while True:
            data = f.read(8192)
            if not data:
                break
            md5_hash.update(data)
        md5_value = md5_hash.hexdigest()
    return md5_value

def remove_unprintable_chars(input_str): #把不能打印出来的字符删掉，否则print屏幕的时候，直接报错中断
    # 使用可打印字符的Unicode范围过滤字符串
    printable_chars = [char for char in input_str if char.isprintable()]
    # 将过滤后的字符重新连接成一个字符串
    filtered_str = ''.join(printable_chars)
    return filtered_str

def cmp_files(file1,file2): #通过查看文件的inode number，比较两个文件，是否是同一个文件
    file1_stat=os.stat(file1)
    file2_stat = os.stat(file2)
    if not file1_stat.st_size==file2_stat.st_size: #如果两个文件大小不等，直接判断不是一个文件
        return False
    if file1_stat.st_ino==file2_stat.st_ino: #如果前面大小想等，那么这里判断是不是inode number一样
        return True
    else:
        return False

if __name__=='__main__':
    file1=r"D:\\wechat2\\WeChat Files\\wxid_cpyn7pe119rs21\\Applet\\wx0bc2c17d023b213d\\usrmmkvstorage0\\wx0bc2c17d023b213d.crc"
    file2=r"D:\\wechat2\\WeChat Files\\wxid_cpyn7pe119rs21\\Applet\\wxff2aab9aa679ef93\\usrmmkvstorage1\\wxff2aab9aa679ef93.crc"
    print(cmp_files(file1,file2))