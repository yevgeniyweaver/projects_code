<?php

namespace App\Services\Crawler\TrafficSourceProviders;

use App\Models\TrafficSources;
use Exception;
use Illuminate\Http\File;

class FileTrafficSourceProvider implements TrafficSourcesProviderInterface
{
    private const PRIORITY = 100;
    private const PRE_MAX_PRIORITY = 70;
    private const DEFAULT_SOURCES_KEY = 'source';
    const ALLOWED_FORMATS = ['xls', 'xlsx', 'txt', 'csv'];

    private ?File $file;


    /**
     * @param File $file
     * @return FileTrafficSourceProvider
     */
    public function setFile(File $file): self
    {
        $this->file = $file;
        return $this;
    }

    /**
     * @return string[]
     * @throws Exception
     */
    public function getSources(int $limit = 0): array
    {
        $this->checkFile();
        $data = $this->getArrayFromSourceFile();
        return count($data) ? $data : [];
    }

    public function getPriority(): int
    {
        return self::PRIORITY;
    }

    public function getSourcesKey(): string
    {
        return self::DEFAULT_SOURCES_KEY;
    }

    public function getArrayFromSourceFile()
    {
        $filePath = $this->file->getPathname();

        if (!file_exists($filePath)) {
            return false;
        }

        $extension = $this->file->getExtension();

        switch ($extension) {
            case 'xls':
            case 'xlsx':
                $data = $this->parseExcel($filePath);
                break;
            case 'csv':
            case 'txt':
                $rawBody = file_get_contents($filePath);
                $data = !empty($rawBody) ? explode("\n", $rawBody) : [];
                $data = isset($data[1]) && !empty($data[1]) ? $data : [];
                break;
            default:
                die('Wrong content type! Please, upload file *.txt, *.csv, *.xls or *.xlsx');
        }

        if (empty($data)) {
            return false;
        }

        if (is_array($data)) {
            $modifyData = array_map(function($item) {
                // if $data from xls,xlsx
                if (is_array($item)) {
                    return str_replace(["www.", "https://", "http://", "http", "https","\r","\n"], '', $item[0]);
                }
                return str_replace(["www.", "https://", "http://", "http", "https","\r","\n"], '', $item);
            }, $data);
        }

        $arrayReturn = [];
        if (!empty($modifyData)) {

            $filteredData = array_diff($modifyData, array(null,"\r","\n", " ", ""));
            if (count($filteredData)) {
                $sourcesExist = (new TrafficSources)->select('source','priority')->whereIn('source', $filteredData)->get()->keyBy('source')->toArray();

                $columnKey = $this->getSourcesKey();
                foreach ($filteredData as $k => $stringOption) {
                    $arrayReturn[$k][$columnKey] = mb_strtolower(clean($stringOption));
                }

                return collect($arrayReturn ?? [])
                        ->pluck('isApp', 'source')
                        ->map(static fn($item) => $item === 'app' ? 1 : 0  )
                        ->toArray();
            }
        }

        return $arrayReturn;
    }

    /**
     * @throws Exception
     */
    public function checkFile(): void {
        $extension = $this->file->getExtension();
        if (!in_array($extension, self::ALLOWED_FORMATS)) {
            throw new Exception("Please, upload file *.txt, *.csv, *.xls or *.xlsx");
        }
    }

    private function parseExcel($file)
    {
        try {
            $r = \PhpOffice\PhpSpreadsheet\IOFactory::createReaderForFile($file);
            $rows = $r->load($file)
                ->getActiveSheet()
                ->toArray();
            return $rows;
        } catch (\PhpOffice\PhpSpreadsheet\Reader\Exception $e) {
            return response([
                'status' => 'error',
                'errors' => ['global' => $e->getMessage()]
            ], 400);
        } catch (\PhpOffice\PhpSpreadsheet\Exception $e) {
            return response([
                'status' => 'error',
                'errors' => ['global' => $e->getMessage()]
            ], 400);
        } catch (Exception $e) {
            return response([
                'status' => 'error',
                'errors' => ['global' => $e->getMessage()]
            ], 400);
        }
    }

}
