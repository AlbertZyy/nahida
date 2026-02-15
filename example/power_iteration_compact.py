
import numpy as np
import nahida as nh


@nh.nodal
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


@nh.nodal
def compute_delta(lam, lam_prev):
    return abs(lam - lam_prev)


step_group = nh.graph(
    power_step(A=nh.gin["A"], x=nh.gin["x"]) \
    >> compute_delta(lam=power_step[1], lam_prev=nh.gin["lambda_prev"]),
    {
        "x_next": power_step[0],
        "lambda": power_step[1],
        "delta": compute_delta
    }
).group()


g_main = nh.graph(
    nh.repeat(
        range(100),
        step_group >> nh.branch(
            nh.formula(
                "d < eps",
                d=step_group["delta"],
                eps=nh.gin['epsilon']
            ),
            true = nh.Break()
        )
    ),
    {
        "lambda": step_group["lambda"],
        "x": step_group["x_next"],
        "delta": step_group["delta"]
    }
)


step_group.subs(
    A=nh.gin['A'],
    x=step_group["x_next"] | nh.gin['x0'],
    lambda_prev=step_group["lambda"] | nh.gin['lambda0']
)


scheduler = nh.ConcurrentScheduler()
executor = nh.ThreadPoolExecutor(4)

func = g_main.lambdify(
    scheduler=scheduler,
    executor=executor
)


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
        epsilon=1e-7
    )

    print("Estimated dominant eigenvalue:", result["lambda"])
    print("Final delta:", result["delta"])
