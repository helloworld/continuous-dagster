import os
import subprocess

import click
from click.testing import CliRunner
from dagster_aws.cli.cli import ensure_requirements, remove_ssh_key

from dagster import seven

TEST_PEM_PRIVATE_KEY = b'''-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAkMDeEQM0+z5OK15HPo/HFWA2deTOr9jFCT429CXJ1jEpcgyVixvVRXRqKrTi
otbRtqwzf5ZLi1/Jl04E5i4xtOvchgV4EHsBUy2pfv2AptBk0MWglviZrlqsNqtCB5JrNVo1dRU0
2cKEytaFZpkB5PYpVyts+XqEy4rADsG42FSAgTmSIS1YW3ts94OCuV2dTs3Rn2YVOE08Yl+Jzw/h
tifgrmSioIIZ82t/x02dgitZ+lhu7SSSuC1bNMXIiX5xd8Kt/eBZLV6cOO6s8jPqfokgADkCe/B2
0k0W2+croulAaGYyaFkzgfGclpYwh48j4tN1GAA+L/73jz4+m0MpBwIDAQABAoIBAQCMoTOdJ5Zz
eIaND5z7pMdvdvex9cbvhBN49V6CN6wtUbnIOX2XlkTOo1j5gJ6mQTmg1Q97JtLh5v985alQzxQv
hu3JrsqEiFwzKSuAWlyAs/kd8sIfqgn0H9crjZoqrQ9G2AsTYrTj0l0ciukzUfdLL/Pd4iyO+37E
GFqae45Pa9PrqLty/MwMpetRMxBd/ga76tbiIlskzJ0TWRN1n9vzmjxZsv7wvnLB6Lb9E2M7IbcF
4jT59PO9PDikyxXSCggtZ8zTOwwfJ0zT1ca0I8V5fAy8+8/gEipPS2w4FDXHDNZYTFjPkAbIcVjk
VOs1gIXnVU9Udx5ZbqYyAc6dIXERAoGBAP+3Mg7AK54UbxDHE+vZ90kWf4zaNCrzlIa6hGO4UGfi
Hu4Q2yi7N4/PFPzqK2fiDmF3KcADDFoalpBjgrkuCwN/mzI5DTmXezC+ybwWDHv5LOpxKSt6jI3k
Cl7gPluSFlpO1PZLqvCoD/2sYEC5UMOtdm3mAbdRmAe0DZWy8KijAoGBAJDqFHrIotedciYrkbwy
ZvH8U4AatgHRNwRyM+pOSPh1mGIsxCLU7/JNNO85Lsj7sY9vrUbNCoUMh3tW7aIPqSM7984BN3OH
j5BDI2UNKjMU2I+/a8hSevBpVAiuXwZdWho2R9X+bOQEJ5/XSCcKb2IEsxnsvUZuJ8PDIUgydNBN
AoGAChAYfTIcxUzCjiN3ajmanJqxDEbt9J6/QooGebIgH+ZrFy08op6zcgpRJh7E9IKV1EoPL4ov
K0COmkIAO+9O6aVU4yYRmimW5HUtlSQ8+4fzt4oad6aL722A5vjmG05laMpfYQ3bPTsv3eixpPpl
7j1eQrbhbGheljcErdZr2AcCgYBkeDX8rq/f50Pl3N0sapFfAj7uGRbJCcEoLvl5YA9NW1Jr9neB
Yrp89jRWCwI5y+7d8GJlLPE3L2mbBLi6XDs6t2/l/ofwbDMHpNScUdVJFxHSD7ftaencVloxrpsp
MX2voKSLY9sg9xR1yG84yg5RGcsGlVDgzKx7GAUyJfFBHQKBgB+xoOvKJRalORPxryV5g37q/0C9
/HqpGCbVxGTuKdq9aASuOae3Lxf6b7NyLVjPLnhdOCwddi8tFbPoZa+3/EBSX1VPVxXAJ/FpkIbW
/HUvDDKSnDcBzS1/ufsTgRO4OqtTrj9idqoyAPar4BdC1ABDbL4OOKP9QAPrvjefBZtt
-----END RSA PRIVATE KEY-----'''


def test_ensure_requirements():
    runner = CliRunner()
    with runner.isolated_filesystem() as tmp_dir:

        @click.command()
        def test():
            ensure_requirements(tmp_dir)

        result = runner.invoke(test)
        assert 'No requirements.txt found, creating' in result.output

        with open(os.path.join(tmp_dir, 'requirements.txt'), 'w') as f:
            f.write('dagster\ndagit\n')

        result = runner.invoke(test)
        assert 'Found existing requirements.txt' in result.output

        with open(os.path.join(tmp_dir, 'requirements.txt'), 'w') as f:
            f.write('dagster\n')

        result = runner.invoke(test, input='n\n')
        assert 'Could not find dagit' in result.output
        assert result.exit_code == 1

        result = runner.invoke(test, input='y\n')
        assert 'Could not find dagit' in result.output
        assert result.exit_code == 0

        with open(os.path.join(tmp_dir, 'requirements.txt'), 'w') as f:
            f.write('dagit\n')

        result = runner.invoke(test, input='n\n')
        assert 'Could not find dagster' in result.output
        assert result.exit_code == 1

        result = runner.invoke(test, input='y\n')
        assert 'Could not find dagster' in result.output
        assert result.exit_code == 0


def test_remove_ssh_key():
    # Ensure SSH is running
    subprocess.call(['ssh-agent', '-s'])

    test_key_path = os.path.join(seven.get_system_temp_directory(), 'test.pem')
    with open(test_key_path, 'wb') as f:
        f.write(TEST_PEM_PRIVATE_KEY)

    os.chmod(test_key_path, 0o600)

    subprocess.call(['ssh-add', '-D'])
    assert remove_ssh_key('does/not/matter')

    subprocess.call(['ssh-add', test_key_path])
    assert not remove_ssh_key('/key/does/not/exist.pem')
    assert remove_ssh_key(test_key_path)
