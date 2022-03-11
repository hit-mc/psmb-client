import pathlib

from setuptools import find_packages, setup

here = pathlib.Path(__file__).parent.resolve()

long_description = (here / 'README.md').read_text(encoding='utf-8')

github_address = 'https://github.com/hit-mc/psmb-client'

setup(
    name='psmb-client',
    version='0.0.4',
    description='psmb client implemented in Python',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url=github_address,
    author='inclyc',
    author_email='axoford@icloud.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: Chinese (Simplified)',
        'Natural Language :: English',
        'Operating System :: MacOS',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3 :: Only',
        'Topic :: Utilities',
    ],
    keywords='Chattings, PSMB, Minecraft, BungeeCross, CrossLink',
    package_dir={'psmb_client': 'psmb_client'},
    packages=find_packages('.'),
    python_requires='>=3.8, <4',
    install_requires=[],
    project_urls={
        'Bug Reports': github_address + '/issues',
        'Source': github_address,
    },
)
