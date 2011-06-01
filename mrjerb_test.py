import copy
import subprocess
import unittest

import mox

import mrjerb

class MRJobStreamingLauncherTestCase(mox.MoxTestBase):
  """Test case for mrjerb.MRJobStreamingLauncher
  """
  def testRunMRJobStreamingJob(self):
    """Test mrjerb.MRJobStreamingLauncher.RunMRJobStreamingJob
    """
    self.mox.StubOutWithMock(subprocess, 'check_call')

    launcher = mrjerb.MRJobStreamingLauncher()
    input_path = '/a/b/c/d'
    input_path2 = 'w/x/y/z'
    output_path = 'e/f/g/h'
    mrjob_file = 'mrjob_file.py'
    reporting_jar = 'streaming_jar'
    archive_file = '/tmp/code.tar.gz'
    job_name = 'DNA'
    partitioner_class = 'com.foo.bar.baz'

    python_path = 'code/tellapart/gen-py:code/tellapart/py'

    base_arr = \
        ['python', mrjob_file, '-r hadoop', '-o ', output_path,
         '--cmdenv \"PYTHONPATH=%s"' % python_path,
         '--archive=%s#code' % archive_file]

    reporting_arr = ['--archive=%s' % reporting_jar]

    jobconf_arr = ['--jobconf="mapred.output.compress=false"',
                   '--jobconf="mapred.job.name=%s"' % job_name]

    num_reduce_tasks_arr = ['--jobconf="mapred.reduce.tasks=37"']

    hadoop_extra_args = \
          ['--hadoop_extra_arg=-libjars %s' % reporting_jar,
           '--hadoop_extra_arg=-partitioner %s' % partitioner_class]

    normal_command_additions = ['--cleanup=ALL']

    expected_hadoop_rm_cmd_arr = ['hadoop', 'fs', '-rmr', output_path]

    expected_cmd_arr = copy.deepcopy(base_arr)
    expected_cmd_arr.extend(jobconf_arr)
    expected_cmd_arr.extend(normal_command_additions)
    expected_cmd_arr.append(input_path)
    expected_string = ' '.join(expected_cmd_arr)
    subprocess.check_call(expected_string, shell=True).AndReturn(None)

    expected_cmd_arr = copy.deepcopy(base_arr)
    expected_cmd_arr.extend(jobconf_arr)
    expected_cmd_arr.extend(normal_command_additions)
    expected_cmd_arr.append(input_path)
    expected_cmd_arr.append(input_path2)
    expected_string = ' '.join(expected_cmd_arr)
    subprocess.check_call(expected_string, shell=True).AndReturn(None)

    expected_cmd_arr = copy.deepcopy(base_arr)
    expected_cmd_arr.extend(reporting_arr)
    expected_cmd_arr.extend(jobconf_arr)
    expected_cmd_arr.extend(num_reduce_tasks_arr)
    expected_cmd_arr.extend(hadoop_extra_args)
    expected_cmd_arr.extend(normal_command_additions)
    expected_cmd_arr.append(input_path)
    expected_string = ' '.join(expected_cmd_arr)
    subprocess.check_call(expected_string, shell=True).AndReturn(None)

    expected_cmd_arr = copy.deepcopy(base_arr)
    expected_cmd_arr.extend(reporting_arr)
    expected_cmd_arr.extend(jobconf_arr)
    expected_cmd_arr.extend(num_reduce_tasks_arr)
    expected_cmd_arr.extend(hadoop_extra_args)
    expected_cmd_arr.append('--output-protocol=raw_value')
    expected_cmd_arr.append('--cleanup=NONE')
    expected_cmd_arr.append(input_path)
    subprocess.check_call(' '.join(expected_hadoop_rm_cmd_arr),
                          shell=True).AndReturn(None)
    expected_string = ' '.join(expected_cmd_arr)
    subprocess.check_call(expected_string, shell=True).AndReturn(None)

    self.mox.ReplayAll()

    launcher.RunMRJobStreamingJob(input_path, output_path, mrjob_file,
                                  archive_file, job_name, python_path)

    launcher.RunMRJobStreamingJob([input_path, input_path2], output_path,
                                  mrjob_file, archive_file, job_name,
                                  python_path)

    launcher.RunMRJobStreamingJob(input_path, output_path,
                                  mrjob_file, archive_file, job_name,
                                  python_path,
                                  num_reduce_tasks_override=37,
                                  jar_paths=[reporting_jar],
                                  partitioner_class=partitioner_class)

    launcher.RunMRJobStreamingJob(input_path, output_path,
                                  mrjob_file, archive_file, job_name,
                                  python_path,
                                  num_reduce_tasks_override=37,
                                  jar_paths=[reporting_jar],
                                  partitioner_class=partitioner_class,
                                  output_protocol_override='raw_value',
                                  cleanup_override='NONE',
                                  delete_output_path=True)

if __name__ == '__main__':
  unittest.main()
