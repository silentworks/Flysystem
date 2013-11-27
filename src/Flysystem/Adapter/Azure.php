<?php

namespace Flysystem\Adapter;

use WindowsAzure\Blob\Models\Block;
use WindowsAzure\Blob\Models\CommitBlobBlocksOptions;
use WindowsAzure\Common\ServiceException;
use WindowsAzure\Common\ServicesBuilder;
use Flysystem\AdapterInterface;
use Flysystem\Util;

class Azure extends AbstractAdapter
{
    protected static $resultMap = array(
        'ContentLength' => 'size',
        'ContentType'   => 'mimetype'
    );

    /**
     * Upload container
     * @var string
     */
    protected $container;
    
    /**
     * Overwrite existing files?
     * @var \WindowsAzure\Blob\Internal\IBlob $client
     */
    protected $client;
    protected $prefix;
    protected $options;

    public function __construct($connectionString = null, $container, $prefix = null, array $options = array())
    {
        $this->container = $container;
        $this->prefix = $prefix;
        $this->client = ServicesBuilder::getInstance()->createBlobService($connectionString);
        $this->options = new CommitBlobBlocksOptions(); 
    }

    public function has($path)
    {
        return $this->client->doesObjectExist($this->container, $this->prefix($path));
    }

    public function write($path, $contents, $visibility)
    {
        $options = $this->getOptions($path, array(
            'Body' => $contents,
            'ContentType' => Util::contentMimetype($contents),
            'ContentLength' => Util::contentSize($contents),
            'ACL' => $visibility === AdapterInterface::VISIBILITY_PUBLIC ? 'public-read' : 'private',
        ));

        $this->upload($contents, $path);
        $options['visibility'] = $visibility;

        return $this->normalizeObject($options);
    }

    public function update($path, $contents)
    {
        return $this->write($path, $contents);
    }

    public function read($path)
    {
        $options = $this->getOptions($path);
        $result = $this->client->getObject($options);

        return $this->normalizeObject($result->getAll());
    }

    public function rename($path, $newpath)
    {
        $options = $this->getOptions($newpath, array(
            'Bucket' => $this->container,
            'CopySource' => $this->container.'/'.$this->prefix($path),
        ));

        $result = $this->client->copyObject($options)->getAll();
        $result = $this->normalizeObject($result, $newpath);
        $this->delete($path);

        return $result;
    }

    public function delete($path)
    {
        $options = $this->getOptions($path);

        return $this->client->deleteObject($options);
    }

    public function deleteDir($path)
    {
        $this->client->deleteMatchingObjects($this->container, $this->prefix($path));
    }

    public function createDir($path)
    {
        return array('path' => $path, 'type' => $dir);
    }

    public function getMetadata($path)
    {
        $options = $this->getOptions($path);
        $result = $this->client->headObject($options);

        return $this->normalizeObject($result->getAll(), $path);
    }

    public function getMimetype($path)
    {
        return $this->getMetadata($path);
    }

    public function getSize($path)
    {
        return $this->getMetadata($path);
    }

    public function getTimestamp($path)
    {
        return $this->getMetadata($path);
    }

    public function getVisibility($path)
    {
        $options = $this->getOptions($path);
        $result = $this->client->getObjectAcl($options)->getAll();

        foreach ($result['Grants'] as $grant) {
            if (isset($grant['Grantee']['URI']) and $grant['Grantee']['URI'] === Group::ALL_USERS) {
                if ($grant['Permission'] !== Permission::READ) {
                    break;
                }

                return ['visibility' => AdapterInterface::VISIBILITY_PUBLIC];
            }
        }

        return ['visibility' => AdapterInterface::VISIBILITY_PRIVATE];
    }

    public function setVisibility($path, $visibility)
    {
        $options = $this->getOptions($path, array(
            'ACL' => $visibility === AdapterInterface::VISIBILITY_PUBLIC ? 'public-read' : 'private',
        ));

        $this->client->putObjectAcl($options);

        return compact('visibility');
    }

    public function listContents()
    {
        $result = $this->client->listBlobs($this->container)->getBlobs();

        if (empty($result)) {
            return array();
        }

        $result = array_map(array($this, 'normalizeObject'), $result);

        return Util::emulateDirectories($result);
    }

    protected function normalizeObject($blob, $path = null)
    {
        /* @var \WindowsAzure\Blob\Models\Blob $blob */
        $result = array('path' => $path ?: $blob->getName());

        $object = $blob->getProperties();
        $modified = $object->getLastModified();
        if ($modified) {
            $result['timestamp'] = $modified->getTimestamp();
        }

        $maps = array(
            'ContentLength' => $object->getContentLength(),
            'ContentType'   => $object->getContentType()
        );

        $result = array_merge($result, Util::map($maps, static::$resultMap), ['type' => 'file']);

        return $result;
    }

    protected function getOptions($path, array $options = array())
    {
        $options['Key'] = $this->prefix($path);
        $options['Bucket'] = $this->container;

        return array_merge($this->options, $options);
    }

    protected function prefix($path)
    {
        if (! $this->prefix) {
            return $path;
        }

        return $this->prefix.'/'.$path;
    }

    protected function upload($content, $path)
    {
        // Make sure the filename is valid for Azure and in lowercase
        $path = Util::slug($path, true, '_');

        $newFile = $this->prefix($path);

        try {
            $handler = fopen($content->getPathname(), 'r');
            $counter = 1;
            $blockIds = [];

            try {
                $this->client->deleteBlob($this->container, $newFile);
            } catch (ServiceException $e) { }

            while (!feof($handler)) {
                $blockId = str_pad($counter, 6, '0', STR_PAD_LEFT);
                $block = new Block();
                $block->setBlockId(base64_encode($blockId));
                $block->setType("Uncommitted");

                array_push($blockIds, $block);
                $data = fread($handler, $this->chunkSize);

                $this->client->createBlobBlock($this->container, $newFile, base64_encode($blockId), $data);

                $counter++;
            }
            fclose($handler);

            return $this->client->commitBlobBlocks($this->container, $newFile, $blockIds, $this->options);
        } catch(ServiceException $e){
            // Handle exception based on error codes and messages.
            // Error codes and messages are here:
            // http://msdn.microsoft.com/en-us/library/windowsazure/dd179439.aspx
            $code = $e->getCode();
            $error_message = $e->getMessage();

            // $file->addError($code.": ".$error_message);
            throw $e;
        }
    }
}
