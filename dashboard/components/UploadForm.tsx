import { FormEvent, useState } from 'react';
import { createS3Job, createUploadJob } from '../lib/api';

interface UploadFormProps {
  onJobCreated: (jobId: string, metadata: { sourceType: string }) => void;
  onUploadFinished?: (jobId: string) => void;
  onError?: (message: string) => void;
}

export function UploadForm({ onJobCreated, onUploadFinished, onError }: UploadFormProps) {
  const [mode, setMode] = useState<'upload' | 's3'>('upload');
  const [file, setFile] = useState<File | null>(null);
  const [s3Path, setS3Path] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [status, setStatus] = useState<string | null>(null);

  const reset = () => {
    setFile(null);
    setS3Path('');
    if (typeof window !== 'undefined') {
      (document.getElementById('dataset-file-input') as HTMLInputElement | null)?.value = '';
    }
  };

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setStatus(null);

    try {
      setSubmitting(true);
      if (mode === 'upload') {
        if (!file) {
          throw new Error('Please choose a dataset to upload.');
        }
        setStatus('Creating job…');
        const response = await createUploadJob();
        onJobCreated(response.jobId, { sourceType: 'upload' });

        if (!response.uploadUrl) {
          throw new Error('Upload URL was not returned by the API.');
        }

        setStatus('Uploading dataset…');
        const uploadResponse = await fetch(response.uploadUrl, {
          method: 'PUT',
          headers: {
            'Content-Type': file.type || 'text/csv',
          },
          body: file,
        });

        if (!uploadResponse.ok) {
          throw new Error(`Upload failed with status ${uploadResponse.status}.`);
        }

        setStatus('Upload complete! Your job has been queued.');
        onUploadFinished?.(response.jobId);
        reset();
        return;
      }

      if (!s3Path.trim()) {
        throw new Error('Please provide an s3:// bucket path.');
      }

      setStatus('Creating job…');
      const response = await createS3Job(s3Path.trim());
      onJobCreated(response.jobId, { sourceType: 's3' });
      setStatus('Job created! We will poll the status automatically.');
      reset();
    } catch (error) {
      console.error('Failed to create job', error);
      const message = error instanceof Error ? error.message : 'Unexpected error creating job';
      setStatus(message);
      onError?.(message);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <form className="glass-panel" onSubmit={handleSubmit}>
      <div className="panel-header">
        <div>
          <p className="eyebrow">Create a job</p>
          <h2>Ingest a new dataset</h2>
          <p className="subtitle">
            Upload a CSV or point to an existing S3 object. We will automatically monitor the job and surface
            the outputs once processing completes.
          </p>
        </div>
        <div className="mode-toggle" role="group" aria-label="Dataset source">
          <button
            type="button"
            className={mode === 'upload' ? 'active' : ''}
            onClick={() => setMode('upload')}
          >
            Direct upload
          </button>
          <button type="button" className={mode === 's3' ? 'active' : ''} onClick={() => setMode('s3')}>
            S3 reference
          </button>
        </div>
      </div>

      {mode === 'upload' ? (
        <div className="form-grid">
          <label className="field">
            <span>Select dataset</span>
            <input
              id="dataset-file-input"
              type="file"
              accept=".csv,text/csv"
              onChange={(event) => setFile(event.target.files?.[0] ?? null)}
            />
          </label>
          <p className="field-hint">
            Files are uploaded securely using a presigned URL that is valid for 15 minutes. Maximum size depends on your
            S3 bucket configuration.
          </p>
        </div>
      ) : (
        <div className="form-grid">
          <label className="field">
            <span>S3 object URI</span>
            <input
              type="text"
              placeholder="s3://my-bucket/path/to/object.csv"
              value={s3Path}
              onChange={(event) => setS3Path(event.target.value)}
            />
          </label>
          <p className="field-hint">
            Provide the fully-qualified path to the object you would like MetricFoundry to process.
          </p>
        </div>
      )}

      <div className="form-actions">
        <button type="submit" disabled={submitting}>
          {submitting ? 'Working…' : mode === 'upload' ? 'Upload & queue job' : 'Queue job'}
        </button>
        {status && <span className="status-message">{status}</span>}
      </div>
    </form>
  );
}

export default UploadForm;
