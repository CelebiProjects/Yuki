"""Yuki CLI entry point."""
import os
import subprocess

import click

from Yuki.utils.env_interpreter import EnvInterpreter
from .server_main import server_start
from .server_main import stop as server_stop
from .server_main import status as server_status

@click.group()
@click.pass_context
def cli(_ctx):
    """ Chern command only is equal to `Chern ipython`
    """

# ------ Server ------ #
@cli.group()
def server():
    """Server management commands."""

@server.command()
def start():
    """Start Yuki server."""
    server_start()

@server.command()
def stop():
    """Stop Yuki server."""
    server_stop()

@server.command()
def status():
    """Check Yuki server status."""
    server_status()

# ------ Docker ------ #
@cli.group()
def docker():
    """Docker management commands."""


def _docker_is_rootless():
    """True if the Docker daemon runs rootless (uid-mapped user namespace)."""
    try:
        result = subprocess.run(
            ['docker', 'info', '--format', '{{json .SecurityOptions}}'],
            capture_output=True, text=True, check=True
        )
        return 'rootless' in result.stdout
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False

@docker.command('run')
@click.argument('image', default='yuki:latest')
@click.option('--yuki-dir', '-d', envvar='YUKIDIR',
              default='~/.Yuki', show_default=True,
              help='Host directory to mount as the container storage '
                   '(/home/yuki/.Yuki, or /root/.Yuki under rootless Docker). '
                   '(env: YUKIDIR).')
@click.option('--port', '-p', envvar='YUKIPORT',
              default='3315', show_default=True,
              help='Host port to map to container port 3315 (env: YUKIPORT).')
@click.option('--dev-dir',
              help='Mount local Yuki source directory as /app/Yuki for development.')
@click.option(
    '--celebi-dir',
    help='Mount local CelebiChrono source directory as /app/CelebiChrono for development.'
)
def docker_run(image, yuki_dir, port, dev_dir, celebi_dir):
    """Run a Yuki Docker container.

    IMAGE is the Docker image name (default: yuki:latest).
    """
    yuki_dir = os.path.expanduser(yuki_dir)
    # Create the storage dir as the invoking user before `docker run`.
    # Otherwise the Docker daemon creates a missing bind-mount source as
    # root, and the non-root container (uid 1000) cannot write to it.
    os.makedirs(yuki_dir, exist_ok=True)
    if _docker_is_rootless():
        # Rootless Docker maps container uid 1000 to an unprivileged host
        # subuid that cannot write the bind-mounted host dir. Container root
        # maps to the invoking host user instead, so run as root and store
        # under /root/.Yuki (HOME for root, where the server looks).
        user_args = ['--user', 'root']
        storage_target = '/root/.Yuki'
    else:
        user_args = []
        storage_target = '/home/yuki/.Yuki'
    cmd = [
        'docker', 'run', '-it', '-d',
        *user_args,
        '-v', f'{yuki_dir}:{storage_target}',
        '-p', f'{port}:3315',
    ]
    if dev_dir:
        dev_dir = os.path.expanduser(dev_dir)
        if not os.path.isdir(dev_dir):
            raise click.ClickException(f"Development directory does not exist: {dev_dir}")
        cmd.extend(['-v', f'{dev_dir}:/mnt/yuki-source:ro'])
    if celebi_dir:
        celebi_dir = os.path.expanduser(celebi_dir)
        if not os.path.isdir(celebi_dir):
            raise click.ClickException(f"CelebiChrono directory does not exist: {celebi_dir}")
        cmd.extend(['-v', f'{celebi_dir}:/app/CelebiChrono'])
    cmd.append(image)
    click.echo(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


@docker.command('restart')
@click.argument('container', required=False)
def docker_restart(container):
    """Sync mounted source into a running Yuki container and restart the server.

    If CONTAINER is omitted, uses the first running container with
    /mnt/yuki-source mounted (or the yuki-dev container name).
    """
    if container is None:
        result = subprocess.run(
            ['docker', 'ps', '--filter', 'volume=/mnt/yuki-source',
             '--format', '{{.Names}}'],
            capture_output=True, text=True, check=False
        )
        containers = result.stdout.strip().split('\n')
        containers = [c for c in containers if c]
        if not containers:
            result = subprocess.run(
                ['docker', 'ps', '--filter', 'name=yuki-dev',
                 '--format', '{{.Names}}'],
                capture_output=True, text=True, check=False
            )
            containers = result.stdout.strip().split('\n')
            containers = [c for c in containers if c]
        if not containers:
            raise click.ClickException(
                "No running Yuki dev container found. "
                "Specify one: yuki docker restart <container>"
            )
        container = containers[0]

    click.echo(f"Syncing source into {container}...")
    subprocess.run(
        ['docker', 'exec', container, 'cp', '-r', '/mnt/yuki-source/.', '/app/Yuki/'],
        check=True
    )
    click.echo("Restarting container...")
    subprocess.run(
        ['docker', 'restart', container],
        check=True
    )
    click.echo("Done.")

# ------ Run workflow ------ #
@cli.command('run-workflow')
@click.argument('workflow_uuid')
@click.option('--cores', '-j', default=None,
              help='Number of cores to pass to snakemake '
                   '(default: runner setting, else all).')
def run_workflow(workflow_uuid, cores):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    """Run a local workflow by its UUID with file staging and status tracking.

    For native workflows this:
    1. Stages in files from Storage to LocalWorkflows using hard links
    2. Executes snakemake with conda environment support
    3. Stages out results back to Storage
    4. Tracks status in the Flask API via results.json

    Files are copied using hard links when on the same filesystem for
    performance, with automatic fallback to regular copy for cross-filesystem.
    """
    from Yuki.kernel.local_execution import (
        execute_workflow, timestamp_logger, workflow_location)
    from Yuki.native_runner import _lock, _set_state
    try:
        yuki_dir = os.path.expanduser(os.environ.get("YUKIDIR", "~/.Yuki"))
        _project_uuid, workflow_path = workflow_location(yuki_dir, workflow_uuid)
        with _lock(os.path.join(workflow_path, "native-runner.lock")) as claimed:
            if not claimed:
                raise click.ClickException("Workflow is already running")
            _set_state(workflow_path, "running")
            try:
                exit_code = execute_workflow(workflow_uuid, cores, timestamp_logger)
            except Exception as exc:
                _set_state(workflow_path, "failed", str(exc))
                raise
    except (OSError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    if exit_code:
        raise click.ClickException(f"Workflow {workflow_uuid} failed with exit code {exit_code}")
    click.echo(f"Workflow {workflow_uuid} completed successfully")


# ------ Impression Import/Export ------ #
@cli.command('impression-export')
@click.argument('impressions', nargs=-1, required=True)
@click.option('--project-uuid', required=True,
              help='Project UUID that the impressions belong to.')
@click.option('--output', '-o', required=True,
              help='Output tar.gz file path.')
@click.option('--yuki-dir', '-d', envvar='YUKIDIR',
              default='~/.Yuki', show_default=True,
              help='Yuki storage directory (env: YUKIDIR).')
def impression_export(impressions, project_uuid, output, yuki_dir):
    """Export one or more impressions to a tar.gz archive.

    IMPRESSIONS are one or more 32-character hex impression UUIDs.
    """
    from Yuki.kernel.impression_transfer import export_impressions

    yuki_dir = os.path.expanduser(yuki_dir)
    export_impressions(project_uuid, list(impressions), output,
                       yuki_dir=yuki_dir)
    click.echo(f"Exported {len(impressions)} impression(s) to: {output}")


@cli.command('impression-import')
@click.argument('tar_file', type=click.Path(exists=True))
@click.option('--project-uuid', required=True,
              help='Target project UUID to import into.')
@click.option('--yuki-dir', '-d', envvar='YUKIDIR',
              default='~/.Yuki', show_default=True,
              help='Yuki storage directory (env: YUKIDIR).')
def impression_import(tar_file, project_uuid, yuki_dir):
    """Import impressions from a tar.gz archive.

    TAR_FILE is the path to the tar.gz archive.
    """
    from Yuki.kernel.impression_transfer import import_impression

    yuki_dir = os.path.expanduser(yuki_dir)
    result = import_impression(project_uuid, tar_file, yuki_dir=yuki_dir)

    click.echo(f"Imported {result['count']} impression(s): "
               f"{', '.join(result['imported'])}")
    if result['skipped']:
        for s in result['skipped']:
            click.echo(f"  Skipped: {s['name']} — {s['reason']}")


# ------ Environment Map ------ #
@cli.group()
def env_map():
    """Manage conda_env_map environment re-interpretations."""


@env_map.command('add')
@click.argument('source', type=str)
@click.argument('env_type', type=str)
@click.argument('value', type=str)
def env_map_add(source, env_type, value):
    """Add or update an environment mapping.

    SOURCE is the original environment string (e.g. docker:img).
    TYPE is the target type (e.g. conda).
    VALUE is the target environment value (e.g. env.yaml).
    """
    yuki_home = os.path.expanduser(os.environ.get("YUKIDIR", "~/.Yuki"))
    config_path = os.path.join(yuki_home, "config.json")
    try:
        EnvInterpreter.add_mapping(config_path, source, env_type, value)
        click.echo(f"Mapped '{source}' -> {env_type}:{value}")
    except (ValueError, TypeError) as e:
        raise click.ClickException(str(e)) from e


@env_map.command('list')
def env_map_list():
    """List all environment mappings."""
    yuki_home = os.path.expanduser(os.environ.get("YUKIDIR", "~/.Yuki"))
    config_path = os.path.join(yuki_home, "config.json")
    try:
        mappings = EnvInterpreter.list_mappings(config_path)
    except (ValueError, TypeError) as e:
        raise click.ClickException(str(e)) from e
    if not mappings:
        click.echo("No environment mappings configured.")
        return
    for source, entry in mappings.items():
        click.echo(f"{source} -> {entry['type']}:{entry['value']}")


@env_map.command('remove')
@click.argument('source', type=str)
def env_map_remove(source):
    """Remove an environment mapping.

    SOURCE is the original environment string to unmap.
    """
    yuki_home = os.path.expanduser(os.environ.get("YUKIDIR", "~/.Yuki"))
    config_path = os.path.join(yuki_home, "config.json")
    try:
        EnvInterpreter.remove_mapping(config_path, source)
        click.echo(f"Removed mapping for '{source}'.")
    except (ValueError, TypeError) as e:
        raise click.ClickException(str(e)) from e


# ------ Runner cache purge ------ #
@cli.command('purge-ssh-runner-cache')
@click.argument('runner')
@click.option('--project', default=None,
              help='Only purge cached impressions of this project.')
@click.option('--impression', default=None,
              help='Only purge this cached impression.')
@click.option('--dry-run', is_flag=True,
              help='List what would be purged without deleting anything.')
@click.option('--yes', '-y', is_flag=True,
              help='Skip the confirmation prompt.')
def purge_ssh_runner_cache(runner, project, impression, dry_run, yes):  # pylint: disable=too-many-locals
    """Evict cached impressions from an ssh runner.

    RUNNER is the runner name from the Yuki registry. The remote
    <remote_workdir>/impressions cache is deleted per entry and the
    local registration bookkeeping (remote.json, status.json,
    distribution.json cache state) is cleared to match. Registered data
    only lives on the runner — restore it afterwards with register-data.
    """
    from Yuki.kernel import runner_config
    from Yuki.kernel.remote_data_ops import purge_runner_cache

    config_file = runner_config.open_config()
    runners_id = config_file.read_variable("runners_id", {})
    if runner not in runners_id:
        raise click.ClickException(f"runner '{runner}' not found")
    runner_id = runners_id[runner]
    backend_types = config_file.read_variable("backend_types", {})
    if not runner_config.is_ssh_backend(backend_types.get(runner_id)):
        raise click.ClickException(f"runner '{runner}' is not an ssh runner")

    if not dry_run and not yes:
        filters = []
        if project:
            filters.append(f"project {project}")
        if impression:
            filters.append(f"impression {impression}")
        scope = f" for {' '.join(filters)}" if filters else ""
        click.confirm(
            f"Purge the impressions cache on ssh runner '{runner}'{scope}?",
            abort=True)

    summary = purge_runner_cache(runner_id, project=project,
                                 impression=impression, dry_run=dry_run,
                                 echo=click.echo)
    for skipped in summary["skipped"]:
        click.echo(f"  Skipped: {skipped['impression']} — "
                   f"{skipped['reason']}")
    if summary["dry_run"]:
        click.echo(f"Dry run — {len(summary['purged'])} cache entr"
                   f"{'y' if len(summary['purged']) == 1 else 'ies'} "
                   f"would be purged, nothing was deleted.")
    else:
        click.echo(f"\n✓ Purged {len(summary['purged'])} cache entr"
                   f"{'y' if len(summary['purged']) == 1 else 'ies'} "
                   f"from runner '{runner}'")
        registered = [e for e in summary["purged"]
                      if e["kind"] == "registered"]
        if registered:
            click.echo("  Registered data lives only on this runner — "
                       "restore it with register-data.")


# Main
def main():
    """Main entry point for Yuki CLI."""
    cli()  # pylint: disable=no-value-for-parameter
