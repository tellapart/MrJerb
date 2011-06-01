import logging
import subprocess
from subprocess import CalledProcessError

class MRJobStreamingLauncher(object):
  def RunMRJobStreamingJob(self, input_path, hdfs_output_path, mrjob_file,
                           archive_file, job_name, python_path,
                           num_reduce_tasks_override=None,
                           jar_paths=None, partitioner_class=None,
                           output_protocol_override=None,
                           cleanup_override=None,
                           delete_output_path=False, compress_output='false',
                           hadoop_binary='hadoop'):
    """Runs a hadoop streaming job using mrjob.
    Args:
      input_path - The data input path. This can be local or hdfs.
                   Hdfs paths need to be prepended with hdfs://... also can be
                   a list of input paths.
      hdfs_output_path - The data output path on hdfs. This does not need
                         hdfs protocol prepended.
      mrjob_file - The location of the mrjob python file
      archive_file - Python archive file containing the python code to be
                     deployed for the streaming job.
      job_name - The name of the MR.
      python_path - The python path to send to the job. This should be in the
                    form 'code/[your pythonpath]'. If there are multiple dirs
                    to put on the pythonpath, separate with colons.
      num_reduce_tasks_override - Use this to override the number of reduce
                                  tasks. Otherwise the default will be used.
      jar_paths - List of paths to any libjars to use. These will be passed with
                  the -libjars command.
      partitioner_class - Override the default HashPartitioner. This needs to
                          be a java class.
      output_protocol_override - Override to the output protocol. Valid values
                                 are json, json_value, pickle, pickle_value,
                                 raw_value, repr, repr_value. Any of the
                                 protocols with value in the name means only
                                 the value will be output (key is lost).
      cleanup_override - Override when this job should cleanup its files on
                         HDFS. This should really only be used for debugging.
                         Valid values are NONE, IF_SUCCESSFUL, SCRATCH, ALL.
      delete_output_path - If set, output path will be deleted prior to launch.
      compress_output - Set to 'true' if output should be compressed. Note that
                        this parameter is a string and not a boolean.
      hadoop_binary - Path to the hadoop binary. Must be set if output paths
                      should be deleted.
    """
    if isinstance(input_path, str):
      input_paths = input_path
    else:
      input_paths = ' '.join(input_path)
    command = ['python', mrjob_file, '-r hadoop', '-o ', hdfs_output_path,
        '--cmdenv \"PYTHONPATH=%s\"' % python_path]

    command_additions = []

    if output_protocol_override is not None:
      command_additions.append('--output-protocol=%s' %
                                output_protocol_override)
    if cleanup_override is None:
      command_additions.append('--cleanup=ALL')
    else:
      command_additions.append('--cleanup=%s' % cleanup_override)

    # Append the name code to the archive file so it's referencible.
    archive_file = '%s#code' % archive_file
    files_arg = ['--archive=%s' % (archive_file)]

    jobconf_arr = ['--jobconf="mapred.output.compress=%s"' % compress_output,
                   '--jobconf="mapred.job.name=%s"' % job_name]
    if num_reduce_tasks_override:
      jobconf_arr.extend(['--jobconf="mapred.reduce.tasks=%s"' % \
                             str(num_reduce_tasks_override)])

    hadoop_extra_args = []
    if jar_paths is not None:
      files_arg.extend(['--archive=%s' % jar_path for jar_path in jar_paths])
      hadoop_extra_args = \
          ['--hadoop_extra_arg=-libjars %s' % ','.join(jar_paths),
           '--hadoop_extra_arg=-partitioner %s' % partitioner_class]

    command.extend(files_arg)
    command.extend(jobconf_arr)
    command.extend(hadoop_extra_args)
    command.extend(command_additions)
    command.append(input_paths)

    if delete_output_path:
      try:
        rm_cmd = [hadoop_binary, 'fs', '-rmr', hdfs_output_path]
        subprocess.check_call(' '.join(rm_cmd), shell=True)
      except CalledProcessError as e:
        logging.error('Caught an exception deleting hdfs path %s. '
                      'Exception: %s' % (hdfs_output_path, e))
        return False


    command_str = ' '.join(command)

    logging.info(command_str)
    try:
      subprocess.check_call(command_str, shell=True)
    except CalledProcessError as exc:
      logging.error("Cannot run MRJob job using %s; "\
                    "skipping (%s)" % (command_str, str(exc)))
      return False
    return True
