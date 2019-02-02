from setuptools import setup
import os

def readme():
    with open('README.md') as f:
        return f.read()


setup(name='ltv-gce',
      version='0.1',
      description='Scripts for running LTV',
      long_description=readme(),
      keywords='lifetime ltv clv gcp',
      url='https://github.com/ophie200/ltv_gce',
      author='Nancy Wong',
      author_email='nawong@mozilla.com',
      license='...',
      packages=['ltv_gce'],
      install_requires=[
          'Lifetimes',
          'dill',
          'numpy',
          'pandas',
          'scipy',
          'google-cloud-logging',
          'google-cloud-storage'
      ],
      test_suite='nose.collector',
      tests_require=['nose', 'nose-cover3'],
      entry_points={
          'console_scripts': ['run-ltv-gce=ltv-gce.ltv_calc:main'],
      },
      include_package_data=True,
zip_safe=False)
