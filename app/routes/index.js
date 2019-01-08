import Router from 'koa-router';
import docker from './docker';
import hadoop from './hadoop';
import spark from './spark';

const router = Router();

router.use('/docker', docker.routes(), docker.allowedMethods());
router.use('/hadoop', hadoop.routes(), hadoop.allowedMethods());
router.use('/spark', spark.routes(), spark.allowedMethods());

export default router;