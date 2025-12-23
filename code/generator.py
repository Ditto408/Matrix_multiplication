# -*- coding: utf-8 -*-
import random
import sys

def generate_matrix(rows, cols, sparsity, output_file):
    """
    生成稀疏矩阵并保存为 CSV: row,col,value
    """
    print("Generating matrix {}x{} with sparsity {}...".format(rows, cols, sparsity))
    total_elements = rows * cols
    num_non_zero = int(total_elements * sparsity)
    
    with open(output_file, 'w') as f:
        # 为了避免内存溢出，我们逐行生成
        for _ in range(num_non_zero):
            r = random.randint(0, rows - 1)
            c = random.randint(0, cols - 1)
            v = random.uniform(1, 100) # 随机浮点数
            # 格式: row,col,value
            f.write("{},{},{:.4f}\n".format(r, c, v))
    print("Saved to {}".format(output_file))

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python generator.py <rows> <cols> <sparsity> <output_path>")
        sys.exit(1)
        
    r = int(sys.argv[1])
    c = int(sys.argv[2])
    s = float(sys.argv[3])
    p = sys.argv[4]
    
    generate_matrix(r, c, s, p)