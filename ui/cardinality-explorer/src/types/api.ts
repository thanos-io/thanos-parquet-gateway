export interface SuccessResponse<T> {
  status: number;
  data: T;
}

export interface ErrorResponse {
  error: {
    code: string;
    message: string;
  };
}

export class APIError extends Error {
  public status: number;
  public code: string;

  constructor({ status, error }: { status: number; error: { code: string; message: string } }) {
    super(error.message);
    this.name = 'APIError';
    this.status = status;
    this.code = error.code;
  }
}
