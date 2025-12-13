#!/usr/bin/env python3
"""
Helper script to deploy Cloud Run Job from YAML config
"""
import yaml
import sys
import subprocess
import os

def deploy_job(job_name, config_file, project_id, image_name):
    """Deploy a Cloud Run Job from config"""
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    
    # Find job config
    job_config = None
    for job in config['jobs']:
        if job['name'] == job_name:
            job_config = job
            break
    
    if not job_config:
        print(f"Error: Job {job_name} not found in config")
        sys.exit(1)
    
    # Extract configuration
    region = job_config.get('region', config.get('default_region', 'asia-southeast1'))
    service_account = job_config.get('service_account', config['default_service_account'].replace('${PROJECT_ID}', project_id))
    memory = job_config['resources']['memory']
    cpu = job_config['resources']['cpu']
    max_retries = job_config['config']['max_retries']
    task_timeout = job_config['config']['task_timeout']
    command = job_config.get('command')
    args = job_config.get('args')
    
    # Build env vars
    env_vars = []
    for key, value in job_config.get('env_vars', {}).items():
        value = value.replace('${PROJECT_ID}', project_id)
        env_vars.append(f"{key}={value}")
    env_vars_str = ','.join(env_vars) if env_vars else None
    
    # Build secrets
    secrets = []
    for key, value in job_config.get('secrets', {}).items():
        secrets.append(f"{key}={value}")
    secrets_str = ','.join(secrets) if secrets else None
    
    # Build labels
    labels = []
    for key, value in job_config.get('labels', {}).items():
        labels.append(f"{key}={value}")
    labels_str = ','.join(labels) if labels else None
    
    # Check if job exists
    check_cmd = [
        'gcloud', 'run', 'jobs', 'describe', job_name,
        '--region', region,
        '--project', project_id
    ]
    
    job_exists = subprocess.run(
        check_cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    ).returncode == 0
    
    # Build deploy command
    if job_exists:
        print(f"Updating job: {job_name}")
        cmd = [
            'gcloud', 'run', 'jobs', 'update', job_name,
            '--image', image_name,
            '--region', region,
            '--service-account', service_account,
            '--memory', memory,
            '--cpu', cpu,
            '--max-retries', str(max_retries),
            '--task-timeout', str(task_timeout),
            '--project', project_id
        ]
        
        if labels_str:
            cmd.extend(['--update-labels', labels_str])
        
        if env_vars_str:
            cmd.extend(['--set-env-vars', env_vars_str])
        
        if secrets_str:
            cmd.extend(['--set-secrets', secrets_str])
        
        if command and command != 'null':
            cmd.extend(['--command', command])
        
        if args and args != 'null':
            cmd.extend(['--args', args])
    else:
        print(f"Creating job: {job_name}")
        cmd = [
            'gcloud', 'run', 'jobs', 'create', job_name,
            '--image', image_name,
            '--region', region,
            '--service-account', service_account,
            '--memory', memory,
            '--cpu', cpu,
            '--max-retries', str(max_retries),
            '--task-timeout', str(task_timeout),
            '--project', project_id
        ]
        
        if labels_str:
            cmd.extend(['--labels', labels_str])
        
        if env_vars_str:
            cmd.extend(['--set-env-vars', env_vars_str])
        
        if secrets_str:
            cmd.extend(['--set-secrets', secrets_str])
        
        if command and command != 'null':
            cmd.extend(['--command', command])
        
        if args and args != 'null':
            cmd.extend(['--args', args])
    
    # Execute deploy
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, check=True)
    
    print(f"✅ Job {job_name} deployed successfully")
    return result.returncode

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Usage: deploy-job.py <job_name> <config_file> <project_id> <image_name>")
        sys.exit(1)
    
    job_name = sys.argv[1]
    config_file = sys.argv[2]
    project_id = sys.argv[3]
    image_name = sys.argv[4]
    
    sys.exit(deploy_job(job_name, config_file, project_id, image_name))

