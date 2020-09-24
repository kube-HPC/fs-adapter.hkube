const pathLib = require('path');
const Stream = require('stream');
const fs = require('fs-extra');
const recursive = require('recursive-readdir');

class FSAdapter {
    constructor() {
        this._wasInit = false;
    }

    async init(config, directories, bootstrap = false) {
        if (!this._wasInit) {
            this._wasInit = true;
            this._baseDirectory = config.baseDirectory;
            this._cleanPathRegex = new RegExp(this._baseDirectory, 'g');
            if (bootstrap) {
                await Promise.all(Object.values(directories).map(dirName => fs.ensureDir(pathLib.join(this._baseDirectory, dirName))));
            }
        }
    }

    async put(options) {
        return this._put({ ...this._parsePath(options.path), ...options });
    }

    async _put(options) {
        const dir = pathLib.join(this._baseDirectory, options.directoryName);
        const file = pathLib.join(dir, options.fileName);
        await fs.ensureDir(dir);
        await this._writeFile(file, options);
        return { path: pathLib.join(options.directoryName, options.fileName) };
    }

    async get(options) {
        const res = await fs.readFile(pathLib.join(this._baseDirectory, options.path));
        return res;
    }

    async getMetadata(options) {
        const { path } = options;
        const stat = await fs.stat(pathLib.join(this._baseDirectory, path));
        const header = await this.seek({ path, start: 0, end: 8 });
        return { size: stat.size, headerInData: true, metadata: { header } };
    }

    async seek(options) {
        const { path, start, end } = options;
        const file = pathLib.join(this._baseDirectory, path);
        const bytesToRead = end - start;
        const position = start;
        const fd = await fs.open(file, 'r');
        let result;
        try {
            result = await fs.read(fd, Buffer.alloc(bytesToRead), 0, bytesToRead, position);
        }
        finally {
            await fs.close(fd);
        }
        return result.buffer;
    }

    async _writeFile(path, options) {
        const { metadata, data } = options;
        if (metadata && metadata.header) {
            const fd = await fs.open(path, 'a');
            try {
                await fs.appendFile(fd, metadata.header);
                await fs.appendFile(fd, data);
            }
            finally {
                await fs.close(fd);
            }
        }
        else {
            await fs.writeFile(path, data);
        }
    }

    _parsePath(fullPath) {
        const seperatedPath = fullPath.split(pathLib.sep);
        const dirName = seperatedPath.slice(0, seperatedPath.length - 1);
        const fileName = seperatedPath[seperatedPath.length - 1];
        return { directoryName: dirName.join(pathLib.sep), fileName };
    }

    async list(options) {
        return this._listFiles(options);
    }

    async listPrefixes(options) {
        const root = pathLib.join(this._baseDirectory, options.path);
        return fs.readdir(root);
    }

    async listWithStats(options) {
        const root = pathLib.join(this._baseDirectory, options.path);
        const statRoot = await fs.stat(root);
        let result;
        if (statRoot.isDirectory()) {
            const maxKeys = options.maxKeys || Number.MAX_SAFE_INTEGER;
            let files = await recursive(root);
            files = files.slice(0, maxKeys);

            result = await Promise.all(files.map(async (p) => {
                const path = p.replace(this._cleanPathRegex, '');
                const stat = await fs.stat(p);
                return { path, size: stat.size, mtime: stat.mtime.toISOString() };
            }));
        }
        else {
            const path = root.replace(this._cleanPathRegex, '');
            result = [{ path, size: statRoot.size, mtime: statRoot.mtime.toISOString() }];
        }
        return result;
    }

    async _listFiles(options) {
        const root = pathLib.join(this._baseDirectory, options.path);
        let exists = await fs.pathExists(root);
        if (exists) {
            const res = await recursive(root);
            return res.map((path) => {
                const filteredPath = path.replace(this._cleanPathRegex, '');
                return { path: filteredPath };
            });
        }

        const seperatedPath = root.split(pathLib.sep);
        const directoryName = (seperatedPath.slice(0, seperatedPath.length - 1)).join(pathLib.sep);

        const prefix = seperatedPath[seperatedPath.length - 1];
        exists = await fs.pathExists(directoryName);
        const result = [];
        if (exists) {
            const files = await fs.readdir(directoryName);
            files.forEach((file) => {
                if (!fs.statSync(pathLib.join(directoryName, file)).isDirectory() && file.startsWith(prefix)) {
                    const fullPath = pathLib.join(directoryName, file);
                    const filteredPath = fullPath.replace(this._cleanPathRegex, '');
                    result.push({ path: filteredPath });
                }
            });
        }
        return result;
    }

    async delete(options) {
        return fs.remove(pathLib.join(this._baseDirectory, options.path));
    }

    async getStream(options) {
        const parsePath = this._parsePath(options.path);
        const file = pathLib.join(this._baseDirectory, parsePath.directoryName, parsePath.fileName);
        return fs.createReadStream(file, { start: options.start, end: options.end });
    }

    async putStream(options) {
        const parsePath = this._parsePath(options.path);
        const dir = pathLib.join(this._baseDirectory, parsePath.directoryName);
        await fs.ensureDir(dir);
        const src = pathLib.join(dir, parsePath.fileName);
        await this._writeStream({ readStream: options.data, src });
        return { path: pathLib.join(parsePath.directoryName, parsePath.fileName) };
    }

    _writeStream({ readStream, src }) {
        return new Promise((resolve, reject) => {
            if (!(readStream instanceof Stream)) {
                reject(new TypeError('data must readable stream'));
            }
            else {
                const writeStream = fs.createWriteStream(src);
                readStream.on('error', (err) => {
                    reject(err);
                });
                writeStream.on('error', (err) => {
                    reject(err);
                });
                writeStream.on('close', () => {
                    resolve();
                });
                readStream.pipe(writeStream);
            }
        });
    }

    getAbsolutePath(path) {
        return pathLib.join(this._baseDirectory, path);
    }
}

module.exports = FSAdapter;
