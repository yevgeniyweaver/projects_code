<?php

namespace App\Http\Controllers\Ssp;

use App\DTO\input\SspByTraffTypesDTO;
use App\Http\Controllers\Controller;
use App\Http\Controllers\SSH2;
use App\Models\BaseModel;
use App\Models\BlockedSSPCrids;
use App\Models\BlockedSSPDomainInAdm;
use App\Models\CookieSyncPartners;
use App\Models\CookieSyncPartnersMap;
use App\Models\DspSspComments;
use App\Models\ExchangeCountryes;
use App\Models\ExternalApiLinksSSP;
use App\Models\ListCustomSSP;
use App\Models\ListDSP;
use App\Models\ListSSP;
use App\Models\Project;
use App\Models\Rates;
use App\Models\RequestExamples;
use App\Models\ResponseExamples;
use App\Models\Servers;
use App\Models\SettingsDSP;
use App\Models\SettingsSSP;
use App\Models\SspPlacementsSettings;
use App\Models\SspRequests;
use App\Models\SspRequestStats;
use App\Models\SspTotalRequest;
use App\Models\SspTrafficQuality;
use App\Models\StatImpression;
use App\Models\Stats;
use App\Models\TrafficQualityTypes;
use App\Models\User;
use App\Services\StorageService;
use Carbon\Carbon;
use ClickHouseDB\Client;
use Exception;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Pagination\Paginator;
use Illuminate\Pagination\LengthAwarePaginator;
use Illuminate\Support\Facades\Auth;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Cookie;
use Illuminate\Support\Facades\Storage;
use Illuminate\Support\Facades\Validator;
use Illuminate\View\View;
use PhpOffice\PhpSpreadsheet\Spreadsheet;
use function app;
use function array_unique;
use function array_values;
use function dd;
use function in_array;

/**
 * Class SspController
 * @package App\Http\Controllers\Ssp
 */
class SspController extends Controller
{

    CONST LIMIT_SERVERS = 10000;
    /**
     * @var array
     */
    private $actionAjax = [
        'getExamples',
        'setCompanyComment',
        'getCompanyComment',
    ];

    /**
     * @var int
     */
    private $limitExportRows = 50000;
    private $storageService;
    private $sspInfoService;

    /**
     * SspController constructor.
     */
    public function __construct(SspInfo $sspInfoService)
    {
       $this->storageService = new StorageService();
       $this->sspInfoService = $sspInfoService;
    }

    public function getIndexTableData(Request $request)
    {
        $cookieStatus = Cookie::get('tbl_ssp_status');
        $cookieIntegration = Cookie::get('tbl_ssp_integration');

        $statusInt = getTblStatusSsp($cookieStatus);
        $statusTxt = getTxtStatus($cookieStatus);
        $integration = getTblIntegration($cookieIntegration);
        $statusIntegration = getTxtIntegration($cookieIntegration);

        $cookieActiveManager = Cookie::get('activeManagerSsp');
        $cookieSelectedRegion = Cookie::get('selectedRegionSsp');
        $sortedBy = $request->get('sortedBy');
        $orderBy = $request->get('orderBy');
        $offset = $request->get('offset');
        $currentPage = (int)$request->page;
        $export = $request->has('export') ? $request->export : false;

        $now = new \DateTime;
        $week_ago = new \DateTime;
        $week_ago->modify('-6 day');
        $from = $week_ago->format('Y-m-d');
        $to = $now->format('Y-m-d');
        $filters = json_decode($request->get('filters'), true);
        $listSSP = (new SettingsSSP)->getListSspWithManagerVue(
            $statusInt,
            $integration,
            $cookieSelectedRegion,
            $cookieActiveManager,
            $orderBy,
            $sortedBy,
            $filters,
            $offset
        );

        $regionQps = (new SettingsSSP)->qpsSumByRegion($statusInt, $integration);
        $totalQps = array_sum($regionQps);
        foreach($regionQps as &$item){
            $item = number_format($item);
        }

        $listManagers = (new User)->getListAccountManagers('ssp');

        if (in_array(app()->project->name, [Project::SMARTY_ADS, Project::GOTHAM_ADS, Project::BIZZ_CLICK, Project::ACUITY])) {
            $winRate = (new Rates)->getWinRate('ssp');
        } else {
            $winRate = (new Rates)->getWinRateOld();
        }

        $listRegions = (new Servers)->getAllRegions();
        $countServers = (new Servers)->getCountServerByRegion();
        $countServers = array_map(function($item){
            $item['countservers'] *= self::LIMIT_SERVERS;
            return $item;
        }, $countServers);

        $trafQualityTypes = TrafficQualityTypes::all()->pluck('type', 'id')->toArray();

        if ($export) {

            $listSSPFull = $listSSP['data'];

            while ($currentPage < $listSSP['last_page']) {
                $currentPage++;
                Paginator::currentPageResolver(function () use ($currentPage) {
                    return $currentPage;
                });
                $listSSP = (new SettingsSSP)->getListSspWithManagerVue(
                    $statusInt,
                    $integration,
                    $cookieSelectedRegion,
                    $cookieActiveManager,
                    $orderBy,
                    $sortedBy,
                    $filters
                );
                $listSSPFull = array_merge($listSSPFull, $listSSP['data']);
            }

            $listSSPFullWithWinRate = [];

            foreach ($listSSPFull as $listItem) {
                $currentStatName = $listItem['statname'];
                if (isset($winRate["$currentStatName"])) {
                    $listItem['win_rate'] = $winRate["$currentStatName"]['winRate'];
                } else {
                    $listItem += ['win_rate' =>  '0' ];
                }
                array_push($listSSPFullWithWinRate, $listItem);
            }

            $headers = [
                'ID',
                'Endpoint name',
                'Status',
                'Pop',
                'Region',
                'Active',
                'QPS',
                'Spend Yesterday',
                'Spend Today',
                'Win Rate',
                'Block by TMT',
                'Company name'
            ];

            $ext = $request->has('ext') ? $request->ext : 'xlsx';
            $columns = [
                'id',
                'statname',
                'transformTraffTypes',
                'ispop',
                'region',
                'active',
                'qps',
                'yesterdayspend',
                'dailyspend',
                'win_rate',
                'blockbytmt',
                'company_name'
            ];

            $title = 'SSP List';
            $this->exportToFile($headers, $columns, $listSSPFullWithWinRate, $title, $ext, 2);

            return response('success', 200);

        } else {

            return response()->json([
                'userRole' => Auth::user()->role,
                'statusTxt' => $statusTxt,
                'statusIntegration' => $statusIntegration,
                'listManagers' => $listManagers ? array_values($listManagers) : null,
                'activeManager' => $listManagers[$cookieActiveManager]['name'] ?? null,
                'listRegions' => $listRegions,
                'selectedRegion' => $cookieSelectedRegion,
                'totalQps' => number_format($totalQps),
                'regionQps' => $regionQps,
                'listSSP' => $listSSP,
                'winRate' => $winRate,
                'dateFrom' => $from,
                'dateTo' => $to,
                'traffTypes' => $trafQualityTypes,
                'countServers' => $countServers
            ]);
        }
    }

    public function getDataByCompany(Request $request, SspList $sspList)
    {
        $cookieStatus = Cookie::get('tbl_ssp_status');

        $statusInt = getTblStatusSsp($cookieStatus);
        $statusTxt = getTxtStatus($cookieStatus);
        $cookieActiveManager = Cookie::get('activeManagerSsp');
        $cookieSelectedRegion = Cookie::get('selectedRegionSsp');
        $sortedBy = $request->get('sortedBy');
        $orderBy = $request->get('orderBy');
        $offset = $request->get('offset');
        $filters = json_decode($request->get('filters'), true);
        $export = $request->has('export') ? $request->export : false;
        $currentPage = (int)$request->page;


        $now = new \DateTime;
        $week_ago = new \DateTime;
        $week_ago->modify('-6 day');
        $from = $week_ago->format('Y-m-d');
        $to = $now->format('Y-m-d');

        $listSSP = (new SettingsSSP)->getListSspWithManager($statusInt);

        $arrayTrafTypes = json_encode((new TrafficQualityTypes)->pluck('type', 'id')->toArray());
        $listSspTrafTypes = (new SspTrafficQuality)->getSspTypes();

        $regionQps = [];
        $totalQps = 0;
        if ($listSSP) {
            $regionQps = [];
            foreach ($listSSP as $sspArr) {
                if (!isset($regionQps[$sspArr['region']])) {
                    $regionQps[$sspArr['region']] = 0;
                }
                $regionQps[$sspArr['region']] += $sspArr['qps'];
            }
            array_walk($regionQps, function(&$value, $key) {
                $value = number_format($value);
            });
            arsort($regionQps);
            $totalQps = number_format(array_sum($regionQps));
        }

        $listManagers = getListCompanyManager($listSSP);

        if (in_array(app()->project->name, [Project::SMARTY_ADS, Project::GOTHAM_ADS, Project::BIZZ_CLICK, Project::ACUITY])) {
            $winRate = (new Rates)->getWinRate('ssp');
        } else {
            $winRate = (new Rates)->getWinRateOld();
        }

        $listRegions = (new Servers)->getAllRegions();
        $listSspCompanyWithEP = $sspList->getEndpointsBySspCompany($request, $statusInt, false, $winRate, $cookieSelectedRegion, $cookieActiveManager, $orderBy, $sortedBy, $filters);

        $trafQualityTypes = TrafficQualityTypes::all()->pluck('type', 'id')->toArray();

        if ($export) {

            $listSSPCompanyFull = $listSspCompanyWithEP['data'];

            while ($currentPage < $listSspCompanyWithEP['last_page']) {
                $currentPage++;
                Paginator::currentPageResolver(function () use ($currentPage) {
                    return $currentPage;
                });
                $listSSPCompany = $sspList->getEndpointsBySspCompany(
                    $request,
                    $statusInt,
                    false,
                    $winRate,
                    $cookieSelectedRegion,
                    $cookieActiveManager,
                    $orderBy,
                    $sortedBy,
                    $filters
                );
                $listSSPCompanyFull = array_merge($listSSPCompanyFull, $listSSPCompany['data']);
            }

            $listSSPFullWithWinRate = [];
            foreach ($listSSPCompanyFull as $companyItem) {
                $listSSPFullWithWinRate[] = $companyItem;
            }

            $headers = [
                'Name',
                'Status',
                'Pop',
                'Region',
                'Active',
                'QPS',
                'Spend Yesterday',
                'Spend Today',
                'Win Rate',
                'Block by TMT',
                'Spend Limit'
            ];

            $ext = $request->has('ext') ? $request->ext : 'xlsx';
            $columns = [
                'company_name',
                'traffTypeNames',
                'ispop',
                'region',
                'active',
                'qps',
                'yesterdayspend',
                'dailyspend',
                'winrate',
                'blockbytmt',
                'spendlimit'
            ];

            $title = 'SSP List';
            $this->exportToFile($headers, $columns, $listSSPFullWithWinRate, $title, $ext, 2);

            return response('success', 200);

        } else {

            return response()->json([
                'userRole' => Auth::user()->role,
                'statusTxt' => $statusTxt,
                'listSspTrafTypes' => $listSspTrafTypes,
                'dateFrom' => $from,
                'dateTo' => $to,
                'totalQps' => $totalQps,
                'regionQps' => $regionQps,
                'listSsp' => $listSspCompanyWithEP,
                'arrayTrafTypes' => $arrayTrafTypes,
                'listManagers' => $listManagers ? array_values($listManagers) : null,
                'activeManager' => $listManagers[$cookieActiveManager]['name'] ?? null,
                'winRate' => $winRate,
                'listRegions' => $listRegions,
                'selectedRegion' => Cookie::get('selectedRegionSsp') ?? null,
                'traffTypes' => $trafQualityTypes
            ]);
        }
    }

    private function prepareListSspCompanyWithEndpoints($listSSP, $winRate)
    {
        $listSspByCompany = [];
        foreach ($listSSP as $sspInfo) {
            $listSspByCompany[$sspInfo['company_name']]['company_name'] = $sspInfo['company_name'];
            $listSspByCompany[$sspInfo['company_name']]['endpoints'][] = $sspInfo;
        }

        foreach ($listSspByCompany as $company => &$row) {
            $row['traffTypeNames'] = implode(',', array_unique(array_column($row['endpoints'], 'traffTypeNames')));
            $regions = array_unique(array_column($row['endpoints'], 'region'));
            sort($regions);
            $row['regions'] = implode(',', $regions);
            $row['active'] = array_sum(array_column($row['endpoints'], 'active'));
            $row['qps'] = array_sum(array_column($row['endpoints'], 'qps'));
            $row['bidqps'] = array_sum(array_column($row['endpoints'], 'bidqps'));
            $row['yesterdayspend'] = array_sum(array_column($row['endpoints'], 'yesterdayspend'));
            $row['dailyspend'] = array_sum(array_column($row['endpoints'], 'dailyspend'));
            $WinRate = [];
            foreach ($row['endpoints'] as $ep) {
                $WinRate[] = $winRate[$ep['statname']]['winRate'] ?? 0;
            }
            $row['avgWinRate'] = array_sum($WinRate) / count($WinRate);
            $row['maxSpendLimit'] = max(array_column($row['endpoints'], 'spendlimit'));
        }

        return $listSspByCompany;
    }

    public function info(Request $request): JsonResponse
    {
        $sspByTraffTypesDTO = SspByTraffTypesDTO::fromRequest($request);
        if (app()->project->name == Project::SMARTY_ADS) {
            $cookieSelectedRegion = Cookie::get('selectedRegionSspInfoTrafTypes');
            $listRegions = (new Servers)->getAllRegions();
        } else {
            $cookieSelectedRegion = null;
            $listRegions = [];
        }

        $listSSP = $this->sspInfoService->getListSspByTraffTypes($sspByTraffTypesDTO, false, false, $cookieSelectedRegion);

        $listManagers = (new User)->getAccountCompany();
        $listRegions = (new Servers)->getAllRegions();

        return response()->json([
            'listSsp' => $listSSP,
            'listManagers' => $listManagers,
            'activeManager' => Cookie::get('activeManager'),
            'listRegions' => $listRegions,
            'selectedRegion' => $cookieSelectedRegion
        ]);
    }
}
