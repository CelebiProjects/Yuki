"""Shell lifecycle fragments for credentials owned by an SSH workflow.

The keytab stays on the runner. Ticket caches are private, ephemeral, and outside
workflow stageout/cache directories. No credential bytes enter Yuki storage.
"""
import shlex


def workflow_kerberos(settings):
    """Return setup/start/cleanup shell fragments; disabled runners are unchanged."""
    if not settings.get("use_kerberos"):
        return "", "", ""
    principal = settings.get("kerberos_principal", "")
    keytab = settings.get("kerberos_keytab", "~/.keytab")
    interval = int(settings.get("kerberos_renew_interval", 1800))
    if not principal or not keytab or interval < 1:
        raise ValueError("SSH Kerberos requires keytab, principal and a positive renew interval")
    # Expand only the leading ~/ on the remote host; never evaluate shell input.
    keytab_expr = ('"$HOME"/' + shlex.quote(keytab[2:])
                   if keytab.startswith("~/") else shlex.quote(keytab))
    setup = r'''krb_dir=""
krb_renew_pid=""
krb_owner=$$
krb_keytab=KEYTAB
krb_principal=PRINCIPAL
krb_refresh() {
    timeout 60 kinit -kt "$krb_keytab" "$krb_principal"
}
krb_initialize() {
    test -r "$krb_keytab" || { echo "Yuki: Kerberos keytab is unreadable" >&2; return 1; }
    krb_dir=$(mktemp -d /tmp/yuki-krb5.XXXXXXXXXX)
    chmod 700 "$krb_dir"
    export KRB5CCNAME="FILE:$krb_dir/ccache"
    krb_refresh
}
krb_renew() {
    # Track sleep so normal completion does not leave a refresh worker behind.
    local sleeper="" failures=0 delay=INTERVAL
    trap 'test -z "$sleeper" || kill "$sleeper" 2>/dev/null || true' EXIT
    trap 'exit 0' TERM INT
    while true; do
        sleep "$delay" &
        sleeper=$!
        wait "$sleeper" || return
        sleeper=""
        if krb_refresh; then
            failures=0
            delay=INTERVAL
        else
            failures=$((failures + 1))
            echo "Yuki: Kerberos refresh failed ($failures/3)" >&2
            if [ "$failures" -ge 3 ]; then
                # yuki_run.sh is launched by setsid: stop this workflow group only.
                kill -TERM -- "-$krb_owner"
                return 1
            fi
            delay=60
        fi
    done
}
'''.replace("KEYTAB", keytab_expr).replace("PRINCIPAL", shlex.quote(principal)).replace(
        "INTERVAL", str(interval))
    start = "krb_initialize\nkrb_renew &\nkrb_renew_pid=$!\n"
    cleanup = r'''    if [ -n "$krb_renew_pid" ]; then
        kill "$krb_renew_pid" 2>/dev/null || true
        wait "$krb_renew_pid" 2>/dev/null || true
    fi
    test -z "$krb_dir" || rm -rf -- "$krb_dir"
'''
    return setup, start, cleanup
