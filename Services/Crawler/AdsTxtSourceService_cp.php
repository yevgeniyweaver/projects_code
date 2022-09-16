<?php

namespace App\Services\Crawler;

use App\Models\AdsTxtContents;
use App\Models\AdsTxtSources;
use App\Services\Crawler\DTO\AdsTxtSourceCrawlerDTO;
use App\Services\Crawler\Exceptions\AdsTxtSourceException;
use Carbon\Carbon;
use GuzzleHttp\Client;
use Illuminate\Support\Str;

class AdsTxtSourceService
{

    private array $pubBlackList = [];

    private const DOMAIN_PROTOCOL = 'http://';

    private const ADS_TXT_HEADERS = [
        'headers' => [
            'User-Agent' => 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36',
            'pragma' => 'no-cache',
            'cache-control' => 'no-cache',
            'accept-language' => 'en-US,en;q=0.9,ru-RU;q=0.8,ru;q=0.7,uk;q=0.6,tr;q=0.5',
            'accept-encoding' => 'gzip, deflate, br',
            'accept' => 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        ],
        'timeout' => 15,
        'verify' => false
    ];

    private bool $needSaveContent = false;
    private bool $needSaveNewSourceId = false;

    public const CRAWLED_STATUS_SUCCESS = 'success';
    public const CRAWLED_STATUS_ERROR = 'fail';

    public array $roles = [self::ROLE_DIRECT, self::ROLE_RESELLER];

    public const ROLE_DIRECT = 'direct';
    public const ROLE_RESELLER = 'reseller';

    protected AdsTxtSourceCrawlerDTO $sourceDTO;
    private Carbon $carbon;
    private Client $client;


    public function __construct(Carbon $carbon, Client $client)
    {
        $this->carbon = $carbon;
        $this->client = $client;
    }

    public function setSource(array $sourceData): self {
        $this->sourceDTO = AdsTxtSourceCrawlerDTO::fromArray($sourceData);
        return $this;
    }

    public function parseAndSaveSource(): void
    {
        try {

            $resultParse = $this->getResultParse();

            if (empty($resultParse)) {
                throw new AdsTxtSourceException('adstxt_nocontent_error');
            }

            $preparedContentsArray = $this->prepareContentToSave($resultParse);

        } catch (\Exception $e) {
            $this->saveSource($e->getMessage());
        }

        if (isset($preparedContentsArray) &&
            !empty($preparedContentsArray) &&
            count($preparedContentsArray)) {
            $this->saveSource();
            $contentsArrayForSave = $this->setContentsSourceId($preparedContentsArray, $this->needSaveNewSourceId);
            $this->saveContentFile($contentsArrayForSave);
        } else {
            $this->saveSource("nofile");
        }
    }

    /**
     * @throws AdsTxtSourceException
     */
    private function getResultParse(): array
    {
        $content = $this->getDomainAdsContent($this->sourceDTO->domain, $this->sourceDTO->path_to_file);

        return $this->getArrayContentFile($content);
    }

    private function saveSource($error = null)
    {
        $idSource = $this->sourceDTO->id;
        $adsTxtSourceIsset = (new AdsTxtSources)->find($idSource);

        if(isset($error) && $adsTxtSourceIsset){
            (new AdsTxtContents)->where("ads_txt_source_id", $idSource)->delete();
        }

        $source = (new AdsTxtSources)->where('domain', $this->sourceDTO->domain)->first();

        if (!$source) {
            $source = (new AdsTxtSources);
            $source->file_hash = '';
        }

        $newStatus = $this->getCrawlingStatus($error);
        $source->crawling_status = $newStatus;
        $source->last_visited = $this->carbon::now()->toDateTimeString();
        $source->domain = $this->sourceDTO->domain;
        $source->path_to_file = $this->sourceDTO->path_to_file;

        if (!$error && $this->sourceDTO->file_hash && $this->sourceDTO->file_hash !== $source->file_hash) {
            $this->needSaveContent = true;
            $source->file_hash = $this->sourceDTO->file_hash;
        }

        if (!$error && $this->sourceDTO->last_modified) {
            $parsedLM = $this->carbon::parse($this->sourceDTO->last_modified);
            $DateTimeLM = $parsedLM->toDateTimeString();
            $source->last_modified = $DateTimeLM ?? null;
        }

        if($error) {
            $source->file_hash = '';
        }

        $source->save();

        if ($source->id != $this->sourceDTO->id) {
            $this->sourceDTO->id = $source->id;
            $this->needSaveNewSourceId = true;
        }

    }

    private function getCrawlingStatus($error = null) {
        $statusInfoNew = [];

        if (isset($error) && !empty($error)) {
            $statusInfoNew['status'] = self::CRAWLED_STATUS_ERROR;
            $statusInfoNew['msg'] = $error;
        } else {
            $statusInfoNew['status'] = self::CRAWLED_STATUS_SUCCESS;
        }
        return json_encode($statusInfoNew);
    }

    private function saveContentFile(array $arrayContents): void
    {
        if ($this->needSaveContent) {
            $idSource = $this->sourceDTO->id;

            (new AdsTxtContents)->where('ads_txt_source_id', $idSource)->delete();
            (new AdsTxtContents)->insert($arrayContents);
        }
    }

    private function prepareContentToSave(array $arrayContent): array
    {
        $arrayPubs = [];
        $arrayReturn = [];

        foreach ($arrayContent as $string) {

            if (stristr($string, '#')) {
                continue;
            }

            $tempArray = explode(",", $string);

            if (count($tempArray) < 2) {
                continue;
            }

            foreach ($tempArray as &$val) {
                $val = trim($val);
            }
            unset($val);

            $arrayPubs[] = $tempArray;
        }

        if (!empty($arrayPubs)) {
            $arrayPubs = array_intersect_key($arrayPubs, array_unique(array_map('serialize', $arrayPubs)));

            foreach ($arrayPubs as &$item) {

                if (count($item) < 3) {
                    continue;
                }

                if(empty($item[0])){
                    continue;
                }

                if (in_array($item[0], $this->pubBlackList)) {
                    continue;
                }

//                if (!checkdnsrr($item[0])) {
//                    continue;
//                }

                $role = strtolower($item[2]);

                if (!in_array($role, $this->roles)) {
                    continue;
                }

                $arrayReturn[] = [
                    'ads_txt_source_id' => $this->sourceDTO->id,
                    'advert' => strtolower($item[0]),
                    'pid' => $item[1],
                    'role' => $role,
                    'tag_id' => $item[3] ?? null,
                ];
            }

            unset($item);
        }

        return $arrayReturn;
    }

    private function setContentsSourceId(array $preparedArray, bool $isNewSource): array {
        if ($isNewSource) {
            $sourceId = $this->sourceDTO->id;
            return array_map(static function($item) use ($sourceId) {
                $item['ads_txt_source_id'] = $sourceId;
                return $item;
            }, $preparedArray);
        }

        return $preparedArray;
    }

    /**
     * @throws AdsTxtSourceException
     */
    private function getDomainAdsContent(string $domainUrl, string $pathToFile = ''): string
    {

        $adsTxtUrl = $this->getAdsTxtUrl($pathToFile, $domainUrl);

        try {

            $resStat = $this->client->request('GET', $adsTxtUrl, self::ADS_TXT_HEADERS);
            $responseCode = $resStat->getStatusCode();

        } catch (\GuzzleHttp\Exception\ConnectException $ce) {
            throw new AdsTxtSourceException('adstxt_connection_error: ' . $ce->getMessage());
        } catch (\GuzzleHttp\Exception\RequestException $ee) {
            throw new AdsTxtSourceException('adstxt_request_error: ' . $ee->getMessage());
        } catch (\Throwable $ex) {
            throw new AdsTxtSourceException('adstxt_global_error: ' . $ex->getMessage());
        }

        if (isset($responseCode) && $responseCode == 200) {

            $content = $resStat->getBody()->getContents();

            if (empty($content)) {
                throw new AdsTxtSourceException('adstxt_nocontent_error' );
            }

            $adsTxtHeaders = $resStat->getHeaders();
            $this->setAdditionalFields($content, $adsTxtUrl, $adsTxtHeaders);

            return $content;
        }

        throw new AdsTxtSourceException('adstxt_code_error: ' . $responseCode ?? 0 );
    }

    private function setAdditionalFields(string $content, string $adsTxtUrl, array $headers) {
        $this->sourceDTO->path_to_file = $adsTxtUrl;
        $this->sourceDTO->file_hash = md5($content);
        $this->sourceDTO->last_modified = $headers['Last-Modified'][0] ?? null;
    }

    private function getAdsTxtUrl(string $pathToFile, $domainUrl): string {
        if (($pathToFile !== 'ads.txt' && $pathToFile !== 'app-ads.txt') &&
            mb_strpos($pathToFile, $domainUrl) !== false
        ) {
            return Str::startsWith($pathToFile, self::DOMAIN_PROTOCOL) ? $pathToFile : self::DOMAIN_PROTOCOL . $pathToFile;

        } else {
            return self::DOMAIN_PROTOCOL . $domainUrl . "/" . $pathToFile;
        }
    }

    /**
     * @throws AdsTxtSourceException
     */
    private function getArrayContentFile(string $text): array
    {
        $data = array_filter(array_map('trim', explode("\n", $text)));

        if (empty($data)) {
            throw new AdsTxtSourceException('adstxt_nocontent_error');
        }

        return array_intersect_key($data, array_unique(array_map('serialize', $data)));
    }
}
