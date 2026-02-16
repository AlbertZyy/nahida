
import numpy as np
import nahida as nh
from nahida.core.expr import FormulaExpr


# ------------------------------
# 1. 单步幂迭代执行节点
# ------------------------------

@nh.nfunc
def power_step(A, x):
    """
    单步幂迭代：
        y = A x
        λ = x^T y
        x_next = y / ||y||
    返回:
        (x_next, lambda_k)
    """
    y = A @ x
    lam = float(np.dot(x, y))
    x_next = y / np.linalg.norm(y)
    return x_next, lam

power_step.set_name("power_step")

@nh.nfunc
def compute_delta(lam, lam_prev):
    return abs(lam - lam_prev)

compute_delta.set_name("compute_delta")

# ------------------------------
# 2. 构造子图（单步迭代）
# ------------------------------

step_node = power_step
delta_node = compute_delta

step_graph = nh.Graph(
    starters=[step_node],
    exposes={
        "x_next": step_node[0],
        "lambda": step_node[1],
        "delta": delta_node
    }
)

step_group = step_graph.group()

step_node.link(delta_node)

step_node.subs(A=step_graph.input["A"], x=step_graph.input["x"])
delta_node.subs(lam=step_node[1], lam_prev=step_graph.input["lambda_prev"])

# ------------------------------
# 3. 主图节点定义
# ------------------------------

repeat = nh.Repeat(range(100))
branch = nh.Branch()
breaker = nh.Break()

# ------------------------------
# 4. 控制流连接
# ------------------------------

# Repeat -> 子图
repeat.iter.link(step_group)

# 子图输出 -> branch
step_group.link(branch)

# 收敛 -> break
branch.true.link(breaker)


# ------------------------------
# 5. 数据订阅
# ------------------------------

g_main = nh.Graph(
    starters=[repeat],
    exposes={
        "lambda": step_group["lambda"],
        "x": step_group["x_next"],
        "delta": step_group["delta"]
    }
)

# 子图订阅主图输入
step_group.subs(
    A=g_main.input['A'],
    x=step_group["x_next"] | g_main.input['x0'],
    lambda_prev=step_group["lambda"] | g_main.input['lambda0']
)

# branch 判断
branch.subs(
    condition=FormulaExpr(
        "d < eps",
        d=step_group["delta"],
        eps=g_main.input['epsilon']
    )
)


# ------------------------------
# 6. 调度与执行
# ------------------------------

scheduler = nh.ConcurrentScheduler()
executor = nh.ThreadPoolExecutor(4)

func = g_main.lambdify(
    scheduler=scheduler,
    executor=executor
)


# ------------------------------
# 7. 运行示例
# ------------------------------

if __name__ == "__main__":

    n = 50
    A = np.random.randn(n, n)
    A = A.T @ A  # 对称正定矩阵

    x0 = np.random.randn(n)
    x0 = x0 / np.linalg.norm(x0)

    result = func(
        A=A,
        x0=x0,
        lambda0=0.0,
        epsilon=1e-3
    )

    print("Estimated dominant eigenvalue:", result["lambda"])
    print("Final delta:", result["delta"])
