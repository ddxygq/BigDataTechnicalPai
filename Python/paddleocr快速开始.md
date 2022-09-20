### 1. ��װPython����

```bash
wget https://mirrors.huaweicloud.com/python/3.8.5/Python-3.8.5.tgz
```



���غ󣬽�ѹ�����룬��װ�����ɡ�

### 2. ��װpaddle-gpu

- ���ĵ���[https://gitee.com/paddlepaddle/PaddleOCR/blob/release/2.5/doc/doc\_ch/quickstart.md](https://gitee.com/paddlepaddle/PaddleOCR/blob/release/2.5/doc/doc_ch/quickstart.md)

- �ٷ��ĵ���[https://www.paddlepaddle.org.cn/install/quick?docurl=/documentation/docs/zh/install/pip/linux-pip.html](https://www.paddlepaddle.org.cn/install/quick?docurl=/documentation/docs/zh/install/pip/linux-pip.html)

����ʹ��gpu�汾����Ҫ��װcuda���Ȳ鿴cuda�汾��

```
[root@tx-sh-orc-001 ocr]# nvidia-smi
Fri Jun 10 14:46:23 2022       
+-----------------------------------------------------------------------------+
| NVIDIA-SMI 410.104      Driver Version: 410.104      CUDA Version: 10.0     |
|-------------------------------+----------------------+----------------------+
| GPU  Name        Persistence-M| Bus-Id        Disp.A | Volatile Uncorr. ECC |
| Fan  Temp  Perf  Pwr:Usage/Cap|         Memory-Usage | GPU-Util  Compute M. |
|===============================+======================+======================|
|   0  Tesla P40           On   | 00000000:00:08.0 Off |                    0 |
| N/A   34C    P0    50W / 250W |  12015MiB / 22919MiB |      0%      Default |
+-------------------------------+----------------------+----------------------+
                                                                               
+-----------------------------------------------------------------------------+
| Processes:                                                       GPU Memory |
|  GPU       PID   Type   Process name                             Usage      |
|=============================================================================|
|    0     22590      C   /usr/local/Python-3.8.5/bin/python3        12005MiB |
+-----------------------------------------------------------------------------+
```

`CUDA Version: 10.0`������汾�ȽϾɣ��������°��ṩ���ǣ�
![](https://oss.ikeguang.com/image/202209201405437.png)

��Ҫע����ǣ�����cuda��10.0�汾�ģ����ٷ����°�linux���֧��cuda10.1��������2�ֽ��������

1). ����cuda�汾������ͼ��cuda10.1 ~ 11.2��Ӣΰ���������cuda��ַ��[https://developer.nvidia.com/cuda-toolkit-archive](https://developer.nvidia.com/cuda-toolkit-archive)��
![](https://oss.ikeguang.com/image/202209201405159.png)

2). �ɰ汾��װ

����ɰ汾��װ�������ҳ�棬��Ϊ���ǵ�cuda��10.0�汾�ģ����������ȫ��������`Ctr + F`��`10.0`���ҵ����
```
python -m pip install paddlepaddle-gpu==2.0.2.post100 -f [https://paddlepaddle.org.cn/whl/mkl/stable.html](https://paddlepaddle.org.cn/whl/mkl/stable.html)
```
![](https://oss.ikeguang.com/image/202209201405047.png)

�ܲ��ң��������ʧ���ˣ�pip�����Ҳ�������汾����ô��Ҫ�Լ��ֶ�����whl�ļ�����װ�ˡ�

�����Ǹ����-f�����������һ����ַ��https://paddlepaddle.org.cn/whl/mkl/stable.html�����ǽ�ȥ��\`Ctr F\`������`2.0.2.post100`��
![](https://oss.ikeguang.com/image/202209201405198.png)

����Ҽ��������ص�ַ��Ȼ��ִ�����

```
pip3 install https://paddle-wheel.bj.bcebos.com/2.0.2/avx/paddlepaddle\_gpu-2.0.2.post100-cp38-cp38-win\_amd64.whl](https://paddle-wheel.bj.bcebos.com/2.0.2/avx/paddlepaddle_gpu-2.0.2.post100-cp38-cp38-win_amd64.whl
```

������Ϊֹ���Ѿ�����90%��

### 3. ��װpaddleocr

��Ϊ�����cuda�汾�Ƚ��ϣ���ά��װ�ģ���Ҳ��̫�����������������paddle-gpu��paddleocr�汾���ȽϾɣ���ô�������ˣ�paddleocr�кܶ�汾�����ǵ���Ӧ�ð�װ�ĸ��汾�����ǣ�ǰ��װ��2.0.2.post100������ȡ2.0.2���ոպã�
```
pip install "paddleocr==2.0.2" # �Ƽ�ʹ��2.0.1+�汾
```

�������Ұ�װ��paddle�汾�ţ������ο���
![](https://oss.ikeguang.com/image/202209201405036.png)