//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package erasure;

public class ReedSolomon {
    private final int dataShardCount;
    private final int parityShardCount;
    private final int totalShardCount;
    private final Matrix matrix;
    private final byte[][] parityRows;

    public ReedSolomon(int dataShardCount, int parityShardCount) {
        this.dataShardCount = dataShardCount;
        this.parityShardCount = parityShardCount;
        this.totalShardCount = dataShardCount + parityShardCount;
        this.matrix = buildMatrix(dataShardCount, this.totalShardCount);
        this.parityRows = new byte[parityShardCount][];

        for(int i = 0; i < parityShardCount; ++i) {
            this.parityRows[i] = this.matrix.getRow(dataShardCount + i);
        }

    }

    public int getDataShardCount() {
        return this.dataShardCount;
    }

    public int getParityShardCount() {
        return this.parityShardCount;
    }

    public int getTotalShardCount() {
        return this.totalShardCount;
    }

    public void encodeParity(byte[][] shards, int offset, int byteCount) {
        this.checkBuffersAndSizes(shards, offset, byteCount);
        byte[][] outputs = new byte[this.parityShardCount][];

        for(int i = 0; i < this.parityShardCount; ++i) {
            outputs[i] = shards[this.dataShardCount + i];
        }

        this.codeSomeShards(this.parityRows, shards, outputs, this.parityShardCount, offset, byteCount);
    }

    public boolean isParityCorrect(byte[][] shards, int firstByte, int byteCount) {
        this.checkBuffersAndSizes(shards, firstByte, byteCount);
        byte[][] toCheck = new byte[this.parityShardCount][];

        for(int i = 0; i < this.parityShardCount; ++i) {
            toCheck[i] = shards[this.dataShardCount + i];
        }

        return this.checkSomeShards(this.parityRows, shards, toCheck, this.parityShardCount, firstByte, byteCount);
    }

    public void decodeMissing(byte[][] shards, boolean[] shardPresent, int offset, int byteCount) {
        this.checkBuffersAndSizes(shards, offset, byteCount);
        int numberPresent = 0;

        for(int i = 0; i < this.totalShardCount; ++i) {
            if (shardPresent[i]) {
                ++numberPresent;
            }
        }

        if (numberPresent != this.totalShardCount) {
            if (numberPresent < this.dataShardCount) {
                throw new IllegalArgumentException("Not enough shards present");
            } else {
                Matrix subMatrix = new Matrix(this.dataShardCount, this.dataShardCount);
                byte[][] subShards = new byte[this.dataShardCount][];
                int subMatrixRow = 0;

                for(int matrixRow = 0; matrixRow < this.totalShardCount && subMatrixRow < this.dataShardCount; ++matrixRow) {
                    if (shardPresent[matrixRow]) {
                        for(int c = 0; c < this.dataShardCount; ++c) {
                            subMatrix.set(subMatrixRow, c, this.matrix.get(matrixRow, c));
                        }

                        subShards[subMatrixRow] = shards[matrixRow];
                        ++subMatrixRow;
                    }
                }

                Matrix dataDecodeMatrix = subMatrix.invert();
                byte[][] outputs = new byte[this.parityShardCount][];
                byte[][] matrixRows = new byte[this.parityShardCount][];
                int outputCount = 0;

                int iShard;
                for(iShard = 0; iShard < this.dataShardCount; ++iShard) {
                    if (!shardPresent[iShard]) {
                        outputs[outputCount] = shards[iShard];
                        matrixRows[outputCount] = dataDecodeMatrix.getRow(iShard);
                        ++outputCount;
                    }
                }

                this.codeSomeShards(matrixRows, subShards, outputs, outputCount, offset, byteCount);
                outputCount = 0;

                for(iShard = this.dataShardCount; iShard < this.totalShardCount; ++iShard) {
                    if (!shardPresent[iShard]) {
                        outputs[outputCount] = shards[iShard];
                        matrixRows[outputCount] = this.parityRows[iShard - this.dataShardCount];
                        ++outputCount;
                    }
                }

                this.codeSomeShards(matrixRows, shards, outputs, outputCount, offset, byteCount);
            }
        }
    }

    private void checkBuffersAndSizes(byte[][] shards, int offset, int byteCount) {
        if (shards.length != this.totalShardCount) {
            throw new IllegalArgumentException("wrong number of shards: " + shards.length);
        } else {
            int shardLength = shards[0].length;

            for(int i = 1; i < shards.length; ++i) {
                if (shards[i].length != shardLength) {
                    throw new IllegalArgumentException("Shards are different sizes");
                }
            }

            if (offset < 0) {
                throw new IllegalArgumentException("offset is negative: " + offset);
            } else if (byteCount < 0) {
                throw new IllegalArgumentException("byteCount is negative: " + byteCount);
            } else if (shardLength < offset + byteCount) {
                throw new IllegalArgumentException("buffers to small: " + byteCount + offset);
            }
        }
    }

    private void codeSomeShards(byte[][] matrixRows, byte[][] inputs, byte[][] outputs, int outputCount, int offset, int byteCount) {
        for(int iByte = offset; iByte < offset + byteCount; ++iByte) {
            for(int iRow = 0; iRow < outputCount; ++iRow) {
                byte[] matrixRow = matrixRows[iRow];
                int value = 0;

                for(int c = 0; c < this.dataShardCount; ++c) {
                    value ^= Galois.multiply(matrixRow[c], inputs[c][iByte]);
                }

                outputs[iRow][iByte] = (byte)value;
            }
        }

    }

    private boolean checkSomeShards(byte[][] matrixRows, byte[][] inputs, byte[][] toCheck, int checkCount, int offset, int byteCount) {
        for(int iByte = offset; iByte < offset + byteCount; ++iByte) {
            for(int iRow = 0; iRow < checkCount; ++iRow) {
                byte[] matrixRow = matrixRows[iRow];
                int value = 0;

                for(int c = 0; c < this.dataShardCount; ++c) {
                    value ^= Galois.multiply(matrixRow[c], inputs[c][iByte]);
                }

                if (toCheck[iRow][iByte] != (byte)value) {
                    return false;
                }
            }
        }

        return true;
    }

    private static Matrix buildMatrix(int dataShards, int totalShards) {
        Matrix vandermonde = vandermonde(totalShards, dataShards);
        Matrix top = vandermonde.submatrix(0, 0, dataShards, dataShards);
        return vandermonde.times(top.invert());
    }

    private static Matrix vandermonde(int rows, int cols) {
        Matrix result = new Matrix(rows, cols);

        for(int r = 0; r < rows; ++r) {
            for(int c = 0; c < cols; ++c) {
                result.set(r, c, Galois.exp((byte)r, c));
            }
        }

        return result;
    }
}
