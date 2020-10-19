from setuptools import setup


setup(
    name='cldfbench_serzantjanicantipassives',
    py_modules=['cldfbench_serzantjanicantipassives'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'cldfbench.dataset': [
            'serzant=cldfbench_serzantjanicantipassives:Dataset',
        ]
    },
    install_requires=[
        'cldfbench',
        'pyglottolog',
    ],
    extras_require={
        'test': [
            'pytest-cldf',
        ],
    },
)
