import subprocess
import sys

def to_str(d):
    return {k: str(d[k]) for k in d}

def run_primary(filename, mode, args):
    c = to_str(args)
    cmd = ['./bench-masstrans-primary',
        c['runtime'], c['init_keys'], c['key_size'], c['val_size'],
        c['txn_size'], c['read_pct'], c['insert_pct'], c['update_pct'],
        c['cross_pct'], c['nthreads'], c['backup_host'], c['start_port']]
    print cmd
    with open(filename, mode) as f:
        subprocess.Popen(cmd, stdout=f, stderr=f).wait()

def run_backup(filename, mode, args):
    c = to_str(args)
    cmd = ['./bench-masstrans-backup',
        c['key_size'], c['val_size'], c['backup_txn_size'],
        c['backup_cross_pct'], c['nthreads'], c['start_port']]
    print cmd
    with open(filename, mode) as f:
        subprocess.Popen(cmd, stdout=f, stderr=f).wait()

def main():
    if sys.argv[1] == 'primary':
        primary = True
    elif sys.argv[1] == 'backup':
        primary = False
    else:
        raise Exception("invalid argument")

    filename = 'synthetic-primary.txt' if primary else 'synthetic-backup.txt'
    c = {}
    c['runtime'] = 15
    c['init_keys'] = 1000000
    c['key_size'] = 16
    c['val_size'] = 64
    c['txn_size'] = 100
    c['backup_txn_size'] = 10
    c['cross_pct'] = 10
    c['backup_cross_pct'] = 10
    c['backup_host'] = '192.168.0.6'
    c['start_port'] = 2000

    for read_pct in [0, 10, 50, 75, 90, 100]:
        rem = 100 - read_pct
        for p in sorted(set([0, rem * 0.25, rem * 0.5, rem * 0.75, rem])):
            update_pct = int(p)
            insert_pct = rem - update_pct
            for nthreads in [8, 16, 24, 32]:
                print read_pct, insert_pct, update_pct, nthreads
                c['read_pct'] = read_pct
                c['insert_pct'] = insert_pct
                c['update_pct'] = update_pct
                c['nthreads'] = nthreads
                if primary:
                    run_primary(filename, 'a', c)
                else:
                    run_backup(filename, 'a', c)

if __name__ == '__main__':
    main()
