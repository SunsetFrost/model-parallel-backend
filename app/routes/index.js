import Router from 'koa-router';
import docker from './docker';

const router = Router();

router.use(docker.routes(), docker.allowedMethods());

export default router;